from neo4j import GraphDatabase
from collections import defaultdict, deque


class BPMNImproperConditionalLivelockDetector:
    """
    Deteksi pola Improper Conditional Livelock (approx. sesuai proposal & graph contoh):

    Pola yang dicari:

    1. AND-join gateway (G_and):
       - Node dengan type 'parallelgateway' (case-insensitive).
       - Memiliki >= 2 incoming edge dengan relationship type 'PARALLELGATEWAY'
         (misalnya dari Task 1 & Task 2).

    2. XOR-join gateway (G_xor):
       - Node dengan type 'exclusivegateway' (case-insensitive).

    3. Terdapat edge langsung:
       - (G_and)-[:SEQUENCE_FLOW]->(G_xor).

    4. G_xor memiliki >= 2 incoming edge total:
       - Satu dari G_and.
       - Minimal satu lagi dari node lain (misalnya Task 3).

    5. (versi ketat, opsional tapi termasuk di sini):
       - G_and & G_xor reachable dari Start.
       - G_xor dapat mencapai End.

    Jika pola di atas terpenuhi → flagged sebagai Improper Conditional Livelock.
    """

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    # ---------------------------------------------------------
    # 1. LOAD GRAPH
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
        Reverse BFS: node mana saja yang bisa mencapai End.
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
    # 3. BFS UNTUK PATH (OPSIONAL, UNTUK OUTPUT)
    # ---------------------------------------------------------
    def _bfs_with_parent(self, start_node, out_edges):
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
    # 4. DETEKSI IMPROPER CONDITIONAL LIVELOCK
    # ---------------------------------------------------------
    def detect_improper_conditional_livelocks(self, process_id):
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

        # 4.1. Identifikasi AND-join parallelgateway (converging)
        and_joins = []  # list of (id_and, par_incoming_sources)
        for nid, info in nodes.items():
            t = (info.get("type") or "").lower()
            if "parallelgateway" not in t:
                continue

            incoming = in_edges.get(nid, [])
            par_in = [src for (src, rtype) in incoming
                      if rtype.upper() == "PARALLELGATEWAY"]
            if len(par_in) >= 2:
                and_joins.append((nid, par_in))

        # 4.2. Identifikasi XOR-join exclusivegateway (tidak perlu cek direction eksplisit)
        xor_gateways = set()
        for nid, info in nodes.items():
            t = (info.get("type") or "").lower()
            if "exclusivegateway" in t:
                xor_gateways.add(nid)

        livelocks = []

        # 4.3. Untuk setiap AND-join, cek apakah dia langsung menuju XOR-join
        for and_id, and_preds in and_joins:
            # AND-join harus reachable dari Start dan bisa menuju End (optional tapi wajar)
            if start_nodes and and_id not in reachable_from_start:
                continue

            # Cari edge AND -> XOR (SEQUENCE_FLOW)
            for (dst, rtype) in out_edges.get(and_id, []):
                if rtype.upper() != "SEQUENCE_FLOW":
                    continue
                if dst not in xor_gateways:
                    continue

                xor_id = dst

                # XOR-join harus punya >=2 incoming (AND + yang lain)
                incoming_xor = in_edges.get(xor_id, [])
                if len(incoming_xor) < 2:
                    continue

                # Harus ada incoming lain selain AND-join
                other_preds = [src for (src, _rt) in incoming_xor if src != and_id]
                if not other_preds:
                    continue

                # XOR juga sebaiknya reachable dari Start dan bisa ke End
                if start_nodes and xor_id not in reachable_from_start:
                    continue
                if end_nodes and xor_id not in can_reach_end:
                    continue

                # Untuk penjelasan, cari path dari masing-masing AND-pred ke AND,
                # lalu dari AND ke XOR, dan (opsional) ke End.
                examples_from_and_preds = {}
                for p in and_preds:
                    # BFS dari p
                    reachable_p, parent_p = self._bfs_with_parent(p, out_edges)
                    if and_id not in reachable_p:
                        continue
                    path_p_to_and = self._reconstruct_path(parent_p, p, and_id)
                    if not path_p_to_and:
                        continue

                    # BFS dari AND ke XOR
                    reachable_and, parent_and = self._bfs_with_parent(and_id, out_edges)
                    if xor_id not in reachable_and:
                        continue
                    path_and_to_xor = self._reconstruct_path(parent_and, and_id, xor_id)
                    if not path_and_to_xor:
                        continue

                    full_path = path_p_to_and[:-1] + path_and_to_xor  # sambung, hilangkan AND duplikat
                    examples_from_and_preds[p] = full_path

                # Contoh path dari salah satu other_pred ke XOR
                examples_from_other = {}
                for op in other_preds:
                    reachable_op, parent_op = self._bfs_with_parent(op, out_edges)
                    if xor_id not in reachable_op:
                        continue
                    path_op_to_xor = self._reconstruct_path(parent_op, op, xor_id)
                    if path_op_to_xor:
                        examples_from_other[op] = path_op_to_xor

                livelocks.append({
                    "and_join": and_id,
                    "xor_join": xor_id,
                    "and_info": nodes.get(and_id, {}),
                    "xor_info": nodes.get(xor_id, {}),
                    "and_predecessors": and_preds,
                    "and_predecessors_info": {p: nodes.get(p, {}) for p in and_preds},
                    "other_predecessors": other_preds,
                    "other_predecessors_info": {p: nodes.get(p, {}) for p in other_preds},
                    "paths_from_and_predecessors": examples_from_and_preds,
                    "paths_from_other_predecessors": examples_from_other,
                })

        return livelocks


# ---------------------------------------------------------
# 5. MAIN DEMO
# ---------------------------------------------------------
if __name__ == "__main__":
    # Sesuaikan koneksi Neo4j
    NEO4J_URI = "bolt://localhost:7687"
    NEO4J_USER = "neo4j"
    NEO4J_PASSWORD = "12345678"

    # process_id dari graph Improper Conditional Livelock yang kamu kirim
    PROCESS_ID = "e100578e-1ae8-4e6a-8dad-f522f4d7b8e9"

    detector = BPMNImproperConditionalLivelockDetector(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    try:
        livelocks = detector.detect_improper_conditional_livelocks(PROCESS_ID)

        if not livelocks:
            print(f"Tidak ditemukan Improper Conditional Livelock untuk process_id={PROCESS_ID}")
        else:
            print(f"Ditemukan {len(livelocks)} Improper Conditional Livelock untuk process_id={PROCESS_ID}:")
            for i, ll in enumerate(livelocks, start=1):
                and_id = ll["and_join"]
                xor_id = ll["xor_join"]
                and_info = ll["and_info"]
                xor_info = ll["xor_info"]

                print(f"\n[Improper Conditional Livelock #{i}]")
                print("  AND-JOIN (parallelgateway, >=2 PARALLELGATEWAY incoming):")
                print(f"    - {and_id}: name={and_info.get('name')}, type={and_info.get('type')}")

                print("  XOR-JOIN (exclusivegateway) yang langsung menerima SEQUENCE_FLOW dari AND-JOIN:")
                print(f"    - {xor_id}: name={xor_info.get('name')}, type={xor_info.get('type')}")

                print("  Predecessor AND-join (harus >=2, via PARALLELGATEWAY):")
                for p in ll["and_predecessors"]:
                    p_info = ll["and_predecessors_info"][p]
                    print(f"    - {p}: name={p_info.get('name')}, type={p_info.get('type')}")

                print("  Predecessor lain dari XOR-join (selain AND-join):")
                for p in ll["other_predecessors"]:
                    p_info = ll["other_predecessors_info"][p]
                    print(f"    - {p}: name={p_info.get('name')}, type={p_info.get('type')}")

                # Tampilkan contoh path (kalau berhasil dihitung)
                if ll["paths_from_and_predecessors"]:
                    print("  Contoh path dari predecessor AND-join ke XOR-join:")
                    for p, path in ll["paths_from_and_predecessors"].items():
                        print(f"    - Dari {p}: {' -> '.join(path)}")

                if ll["paths_from_other_predecessors"]:
                    print("  Contoh path dari predecessor lain ke XOR-join:")
                    for p, path in ll["paths_from_other_predecessors"].items():
                        print(f"    - Dari {p}: {' -> '.join(path)}")
    finally:
        detector.close()
