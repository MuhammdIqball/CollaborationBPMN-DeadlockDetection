from neo4j import GraphDatabase
from collections import defaultdict, deque


class BPMNSourceDeadlockDetector:
    """
    Deteksi Source Deadlock (mendukung kolaborasi / multi-pool):

    - SOURCE S:
      * Node (biasanya Activity / Gateway) dengan >= 2 outgoing edge
        bertipe control-flow:
          type(r) ∈ {"SEQUENCE_FLOW", "EXCLUSIVEGATEWAY", "PARALLELGATEWAY"}.
      * S reachable dari Start Event (supaya benar-benar bisa dieksekusi).

    - JOIN J:
      * Node yang dicapai oleh >= 2 cabang berbeda yang berawal dari SOURCE S:
          S -> child_1 -> ... -> J
          S -> child_2 -> ... -> J
      * J punya indegree >= 2 (titik gabung),
      * dan (opsional) J bisa mencapai End Event.

    Contoh pola pada model:
        S   = aktivitas tempat proses bercabang
        JOIN = aktivitas tempat cabang-cabang tersebut bertemu lagi
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
        Load nodes dan edges untuk process_id tertentu dari Neo4j.

        Return:
            nodes: dict[node_id] = {
                "labels": [...],
                "type": "...",
                "name": "..."
            }
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
                rel_type = record["rel_type"]

                if src in nodes and dst in nodes:
                    out_edges[src].append((dst, rel_type))
                    in_edges[dst].append((src, rel_type))

        return nodes, out_edges, in_edges

    # ---------------------------------------------------------
    # 2. START & END + REACHABILITY
    # ---------------------------------------------------------
    def _find_start_nodes(self, nodes):
        starts = []
        for nid, info in nodes.items():
            t = (info.get("type") or "").lower()
            if "start" in t:
                starts.append(nid)
        return starts

    def _find_end_nodes(self, nodes):
        ends = []
        for nid, info in nodes.items():
            t = (info.get("type") or "").lower()
            if "end" in t:
                ends.append(nid)
        return ends

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
        mencapai End (kalau graph dibalik).
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
    # 3. BFS DARI CHILD UNTUK PATH
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
    # 4. DETEKSI SOURCE DEADLOCK
    # ---------------------------------------------------------
    def detect_source_deadlocks(self, process_id):
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

        # 4.1. Cari SOURCE:
        #      node dengan >= 2 outgoing control-flow:
        #      type(r) ∈ {SEQUENCE_FLOW, EXCLUSIVEGATEWAY, PARALLELGATEWAY},
        #      dan bukan Pool/Lane.
        sources = []  # list of (source_node_id, list_of_children)
        CONTROL_RELS = {"SEQUENCE_FLOW", "EXCLUSIVEGATEWAY", "PARALLELGATEWAY"}

        for nid, outgoing in out_edges.items():
            labels = nodes.get(nid, {}).get("labels", [])
            if "Pool" in labels or "Lane" in labels:
                continue

            children = [
                dst for (dst, rtype) in outgoing
                if (rtype or "").upper() in CONTROL_RELS
            ]

            if len(children) >= 2 and nid in reachable_from_start:
                sources.append((nid, children))

        source_deadlocks = []

        # 4.2. Untuk setiap SOURCE, cari JOIN yang benar
        for s, children in sources:
            child_reachables = {}
            child_parents = {}

            for c in children:
                reachable_c, parent_c = self._bfs_with_parent(c, out_edges)
                child_reachables[c] = reachable_c
                child_parents[c] = parent_c

            candidate_nodes = set()
            for c in children:
                candidate_nodes |= child_reachables[c]

            for j in candidate_nodes:
                if j == s:
                    continue

                # JOIN harus punya indegree >= 2
                if len(in_edges.get(j, [])) < 2:
                    continue

                # JOIN (opsional) bisa mencapai End
                if end_nodes and j not in can_reach_end:
                    continue

                contributing_children = []
                child_paths = {}

                for c in children:
                    if j not in child_reachables[c]:
                        continue
                    parent_c = child_parents[c]
                    path_c_to_j = self._reconstruct_path(parent_c, c, j)
                    if not path_c_to_j:
                        continue

                    # Lengkapi dengan S di depan: S -> c -> ... -> j
                    full_path = [s] + path_c_to_j if path_c_to_j[0] != s else path_c_to_j
                    contributing_children.append(c)
                    child_paths[c] = full_path

                if len(contributing_children) >= 2:
                    source_deadlocks.append({
                        "source": s,
                        "join": j,
                        "source_info": nodes.get(s, {}),
                        "join_info": nodes.get(j, {}),
                        "children": contributing_children,
                        "children_info": {
                            c: nodes.get(c, {}) for c in contributing_children
                        },
                        "paths": child_paths,
                        "nodes": nodes,  # supaya main bisa akses nama semua node
                    })

        return source_deadlocks


# ---------------------------------------------------------
# 5. MAIN: OUTPUT LEBIH MUDAH DIBACA ORANG AWAM
# ---------------------------------------------------------
if __name__ == "__main__":
    # Sesuaikan koneksi Neo4j
    NEO4J_URI = "bolt://localhost:7687"
    NEO4J_USER = "neo4j"
    NEO4J_PASSWORD = "12345678"

    # HARUS sesuai dengan process_id di CQL (perhatikan 'f' di belakang)
    PROCESS_ID = "36fc4ab6-b8e0-40cb-b673-6289db3235df"

    detector = BPMNSourceDeadlockDetector(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    try:
        deadlocks = detector.detect_source_deadlocks(PROCESS_ID)

        if not deadlocks:
            print(f"Tidak ditemukan pola *Source Deadlock* pada process_id = {PROCESS_ID}.")
        else:
            print(f"Ditemukan {len(deadlocks)} pola *Source Deadlock* pada process_id = {PROCESS_ID}.\n")

            for i, dl in enumerate(deadlocks, start=1):
                nodes_map = dl["nodes"]

                s = dl["source"]
                j = dl["join"]
                s_info = dl["source_info"]
                j_info = dl["join_info"]

                source_name = s_info.get("name") or s
                join_name = j_info.get("name") or j

                print("====================================================")
                print(f"[Source Deadlock #{i}]")
                print("Ringkasan pola:")
                print(f"  • Proses mulai bercabang di aktivitas: {source_name} (id={s})")
                print(f"  • Cabang-cabang tersebut bertemu kembali di: {join_name} (id={j})")
                print("  • Struktur seperti ini berpotensi membingungkan alur eksekusi,")
                print("    karena beberapa jalur yang dimulai dari titik yang sama")
                print("    digabung lagi tanpa gateway join yang jelas.\n")

                print("Detail struktur:")
                print("  - Titik sumber percabangan (SOURCE):")
                print(f"      {source_name}  [type={s_info.get('type')}, id={s}]")

                print("  - Titik gabung cabang (JOIN):")
                print(f"      {join_name}  [type={j_info.get('type')}, id={j}]")

                print("\n  Cabang-cabang dari SOURCE yang bertemu kembali di JOIN:")
                for idx, c in enumerate(dl["children"], start=1):
                    c_info = dl["children_info"][c]
                    c_name = c_info.get("name") or c
                    path_ids = dl["paths"][c]

                    # Konversi id → nama (kalau ada, kalau tidak pakai id)
                    path_names = []
                    for nid in path_ids:
                        n_info = nodes_map.get(nid, {})
                        nm = n_info.get("name") or nid
                        path_names.append(nm)

                    print(f"    Cabang {idx}:")
                    print(f"      • Child awal cabang : {c_name}  [type={c_info.get('type')}, id={c}]")
                    print(f"      • Urutan aktivitas  : {' -> '.join(path_names)}")

                print("\nPenjelasan singkat:")
                print(f"  Di aktivitas '{source_name}', alur proses dipecah menjadi beberapa cabang.")
                print(f"  Cabang-cabang itu kemudian digabung lagi di '{join_name}'.")
                print("  Bila tidak dimodelkan dengan gateway yang tepat, struktur ini bisa")
                print("  menimbulkan *source deadlock*: proses tampak memiliki dua alur yang")
                print("  saling bergantung, tetapi tidak jelas aturan eksekusinya.\n")

    finally:
        detector.close()
