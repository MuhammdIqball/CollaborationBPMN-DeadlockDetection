from neo4j import GraphDatabase
from collections import defaultdict, deque


class BPMNImproperStructDeadlockDetector:
    """
    Deteksi Improper Structuring Deadlock (sesuai pola di proposal & graph contoh):

    - XOR-SPLIT (S):
        * Node dengan >= 2 outgoing relationship type 'EXCLUSIVEGATEWAY'
          → implicit XOR-split.
        * S reachable dari Start Event.

    - AND-JOIN (J):
        * Node dengan >= 2 incoming relationship type 'PARALLELGATEWAY'
          → implicit AND-join.
        * J bisa mencapai End Event.

    - Improper Structuring Deadlock terjadi jika:
        * Minimal 2 child langsung dari S (target EXCLUSIVEGATEWAY)
          masing-masing mempunyai path menuju J.
          Contoh: Task 1 → Task 2 → Task 4 dan Task 1 → Task 3 → Task 4.
    """

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    # ---------------------------------------------------------
    # 1. LOAD GRAPH DARI NEO4J
    # ---------------------------------------------------------
    def _load_process_graph(self, process_id):
        """
        Return:
            nodes: dict[node_id] = {"labels": [...], "type": "...", "name": "..."}
            out_edges: dict[node_id] = [(target_id, rel_type_str)]
            in_edges:  dict[node_id] = [(source_id, rel_type_str)]
        """
        nodes = {}
        out_edges = defaultdict(list)
        in_edges = defaultdict(list)

        with self.driver.session() as session:
            # Node
            result = session.run(
                """
                MATCH (n)
                WHERE n.process_id = $process_id
                RETURN n.id AS id, labels(n) AS labels,
                       n.type AS type, n.name AS name
                """,
                process_id=process_id
            )
            for record in result:
                node_id = record["id"]
                nodes[node_id] = {
                    "labels": record["labels"],
                    "type": record["type"],
                    "name": record["name"],
                }

            # Relasi
            result = session.run(
                """
                MATCH (a)-[r]->(b)
                WHERE r.process_id = $process_id
                RETURN a.id AS src, b.id AS dst, type(r) AS rel_type
                """,
                process_id=process_id
            )
            for record in result:
                src = record["src"]
                dst = record["dst"]
                rel_type = record["rel_type"]  # "SEQUENCE_FLOW" / "EXCLUSIVEGATEWAY" / "PARALLELGATEWAY", dll

                if src in nodes and dst in nodes:
                    out_edges[src].append((dst, rel_type))
                    in_edges[dst].append((src, rel_type))

        return nodes, out_edges, in_edges

    # ---------------------------------------------------------
    # 2. START & END
    # ---------------------------------------------------------
    def _find_start_nodes(self, nodes):
        start_nodes = []
        for nid, info in nodes.items():
            t = (info.get("type") or "").lower()
            if "start" in t:
                start_nodes.append(nid)
        return start_nodes

    def _find_end_nodes(self, nodes):
        end_nodes = []
        for nid, info in nodes.items():
            t = (info.get("type") or "").lower()
            if "end" in t:
                end_nodes.append(nid)
        return end_nodes

    def _reachable_from_starts(self, start_nodes, out_edges):
        reachable = set()
        q = deque(start_nodes)
        while q:
            u = q.popleft()
            if u in reachable:
                continue
            reachable.add(u)
            for v, _ in out_edges.get(u, []):
                if v not in reachable:
                    q.append(v)
        return reachable

    def _can_reach_end(self, end_nodes, in_edges):
        """
        Reverse BFS dari end nodes: node mana saja yang bisa
        mencapai end (kalau graph dibalik).
        """
        can_reach = set()
        q = deque(end_nodes)
        while q:
            u = q.popleft()
            if u in can_reach:
                continue
            can_reach.add(u)
            for src, _ in in_edges.get(u, []):
                if src not in can_reach:
                    q.append(src)
        return can_reach

    # ---------------------------------------------------------
    # 3. BFS BIASA DARI SATU NODE
    # ---------------------------------------------------------
    def _bfs_with_parent(self, start_node, out_edges):
        """
        BFS dari start_node, return:
        - reachable: set node yang dapat dicapai
        - parent: map node -> parent untuk rekonstruksi path
        """
        reachable = set()
        parent = {}
        q = deque([start_node])

        while q:
            u = q.popleft()
            if u in reachable:
                continue
            reachable.add(u)
            for v, _rel in out_edges.get(u, []):
                if v not in reachable:
                    parent[v] = u
                    q.append(v)

        return reachable, parent

    def _reconstruct_path(self, parent, src, dst):
        """
        Rekonstruksi path dari src ke dst menggunakan parent[].
        Jika tidak ada path, return None.
        """
        if dst != src and dst not in parent:
            return None
        path = [dst]
        while path[-1] != src:
            p = parent.get(path[-1])
            if p is None:
                return None
            path.append(p)
        path.reverse()
        return path

    # ---------------------------------------------------------
    # 4. DETEKSI IMPROPER STRUCTURING DEADLOCK
    # ---------------------------------------------------------
    def detect_improper_struct_deadlocks(self, process_id):
        nodes, out_edges, in_edges = self._load_process_graph(process_id)

        start_nodes = self._find_start_nodes(nodes)
        end_nodes = self._find_end_nodes(nodes)

        if not start_nodes:
            print("Warning: tidak ada start event.")
        if not end_nodes:
            print("Warning: tidak ada end event.")

        reachable_from_start = (
            self._reachable_from_starts(start_nodes, out_edges)
            if start_nodes else set(nodes.keys())
        )
        can_reach_end = (
            self._can_reach_end(end_nodes, in_edges)
            if end_nodes else set(nodes.keys())
        )

        # 4.1. XOR-SPLIT: node dengan >= 2 outgoing EXCLUSIVEGATEWAY
        xor_splits = []  # list of (split_node_id, list_of_children)
        for nid, outgoing in out_edges.items():
            xor_children = [dst for (dst, rtype) in outgoing
                            if rtype.upper() == "EXCLUSIVEGATEWAY"]
            if len(xor_children) >= 2:
                xor_splits.append((nid, xor_children))

        # 4.2. AND-JOIN: node dengan >= 2 incoming PARALLELGATEWAY
        and_joins = []  # list of (join_node_id, list_of_pred)
        for nid, incoming in in_edges.items():
            and_incoming = [src for (src, rtype) in incoming
                            if rtype.upper() == "PARALLELGATEWAY"]
            if len(and_incoming) >= 2:
                and_joins.append((nid, and_incoming))

        improper_deadlocks = []

        # 4.3. Cek kombinasi XOR-split (S) dan AND-join (J)
        for s, xor_children in xor_splits:
            if start_nodes and s not in reachable_from_start:
                continue

            for j, join_preds in and_joins:
                if j == s:
                    continue

                if end_nodes and j not in can_reach_end:
                    continue

                contributing_children = []
                child_paths = {}

                # Untuk tiap child dari XOR-split, cek apakah child bisa mencapai J
                for c in xor_children:
                    reachable_c, parent_c = self._bfs_with_parent(c, out_edges)
                    if j not in reachable_c:
                        continue

                    path_c_to_j = self._reconstruct_path(parent_c, c, j)
                    if not path_c_to_j:
                        continue

                    # Lengkapi dengan S di depan
                    full_path = [s] + path_c_to_j if path_c_to_j[0] != s else path_c_to_j
                    contributing_children.append(c)
                    child_paths[c] = full_path

                if len(contributing_children) >= 2:
                    improper_deadlocks.append({
                        "split": s,
                        "join": j,
                        "split_info": nodes.get(s, {}),
                        "join_info": nodes.get(j, {}),
                        "children": contributing_children,
                        "children_info": {c: nodes.get(c, {}) for c in contributing_children},
                        "join_predecessors": join_preds,
                        "join_predecessors_info": {p: nodes.get(p, {}) for p in join_preds},
                        "paths": {c: child_paths[c] for c in contributing_children},
                    })

        return improper_deadlocks


# ---------------------------------------------------------
# 5. MAIN – OUTPUT LEBIH ORANG-AWAM FRIENDLY
# ---------------------------------------------------------
if __name__ == "__main__":
    # Sesuaikan koneksi Neo4j
    NEO4J_URI = "bolt://localhost:7687"
    NEO4J_USER = "neo4j"
    NEO4J_PASSWORD = "12345678"

    # process_id dari graph Improper Struct
    PROCESS_ID = "0b230d09-277b-4b6c-87a4-4c81a5ea2937"

    detector = BPMNImproperStructDeadlockDetector(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    try:
        deadlocks = detector.detect_improper_struct_deadlocks(PROCESS_ID)

        if not deadlocks:
            print(f"Tidak ditemukan pola *Improper Structuring Deadlock* pada process_id = {PROCESS_ID}.")
        else:
            print(f"Ditemukan {len(deadlocks)} pola *Improper Structuring Deadlock* pada process_id = {PROCESS_ID}.\n")

            for i, dl in enumerate(deadlocks, start=1):
                split_id = dl["split"]
                join_id = dl["join"]
                split_info = dl["split_info"]
                join_info = dl["join_info"]

                split_name = split_info.get("name") or split_id
                join_name = join_info.get("name") or join_id

                print("====================================================")
                print(f"[Improper Structuring Deadlock #{i}]")
                print("Ringkasan pola:")
                print(f"  • Proses bercabang secara eksklusif (XOR) di: {split_name} (id={split_id})")
                print(f"  • Cabang-cabang tersebut kemudian digabung secara paralel (AND) di: {join_name} (id={join_id})")
                print("  • Kombinasi XOR-split → AND-join pada cabang yang sama ini")
                print("    merupakan struktur yang tidak seimbang (*improper*),")
                print("    dan berpotensi menimbulkan deadlock.\n")

                print("Detail struktur:")
                print("  - XOR-SPLIT (titik cabang, >=2 EXCLUSIVEGATEWAY keluar):")
                print(f"      {split_name}  [type={split_info.get('type')}, id={split_id}]")

                print("  - AND-JOIN (titik gabung, >=2 PARALLELGATEWAY masuk):")
                print(f"      {join_name}  [type={join_info.get('type')}, id={join_id}]")

                print("\n  Cabang XOR yang berkontribusi ke AND-join:")
                for idx, c in enumerate(dl["children"], start=1):
                    c_info = dl["children_info"][c]
                    c_name = c_info.get("name") or c
                    path_ids = dl["paths"][c]

                    # id → nama
                    path_names = []
                    for nid in path_ids:
                        if nid == split_id:
                            nm = split_name
                        elif nid == join_id:
                            nm = join_name
                        elif nid in dl["children_info"]:
                            nm = dl["children_info"][nid].get("name") or nid
                        else:
                            nm = nid
                        path_names.append(nm)

                    print(f"    Cabang {idx}:")
                    print(f"      • Child: {c_name}  [type={c_info.get('type')}, id={c}]")
                    print(f"      • Jalur: {' -> '.join(path_names)}")

                print("\n  Node yang langsung masuk ke AND-join (sumber PARALLELGATEWAY):")
                for p in dl["join_predecessors"]:
                    p_info = dl["join_predecessors_info"][p]
                    p_name = p_info.get("name") or p
                    print(f"      • {p_name}  [type={p_info.get('type')}, id={p}]")

                print("\nPenjelasan singkat:")
                print(f"  Gateway XOR di '{split_name}' memaksa proses memilih salah satu cabang,")
                print(f"  tetapi kemudian cabang-cabang itu digabung lagi di gateway AND '{join_name}'.")
                print("  Secara semantik, ini tidak konsisten dan dapat menyebabkan situasi")
                print("  di mana proses menunggu semua cabang aktif, padahal secara XOR hanya")
                print("  satu cabang yang pernah dijalankan. Itulah yang dikategorikan sebagai")
                print("  *Improper Structuring Deadlock*.\n")

    finally:
        detector.close()
