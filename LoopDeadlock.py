from neo4j import GraphDatabase
from collections import defaultdict, deque

class BPMNDeadlockDetector:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    # ---------------------------------------------------------
    # 1. LOAD GRAPH DARI NEO4J
    # ---------------------------------------------------------
    def _load_process_graph(self, process_id):
        """
        Load nodes dan edges untuk process_id tertentu dari Neo4j dan
        membentuk adjacency list (graph).
        Return:
            nodes: dict[node_id] = {"labels": [...], "type": "..."}
            adj:   dict[node_id] = [successor node_ids]
            rev_adj: reverse adjacency (untuk reachability ke end)
        """
        nodes = {}
        adj = defaultdict(list)
        rev_adj = defaultdict(list)

        with self.driver.session() as session:
            # Ambil semua node dengan process_id tsb
            result = session.run(
                """
                MATCH (n)
                WHERE n.process_id = $process_id
                RETURN n.id AS id, labels(n) AS labels, n.type AS type
                """,
                process_id=process_id
            )
            for record in result:
                node_id = record["id"]
                labels = record["labels"]
                node_type = record["type"]
                nodes[node_id] = {
                    "labels": labels,
                    "type": node_type,
                }

            # Ambil semua edge (sequence flow, gateway, dll) dengan process_id tsb
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
                # hanya kalau kedua node ketemu (defensive)
                if src in nodes and dst in nodes:
                    adj[src].append(dst)
                    rev_adj[dst].append(src)

        return nodes, adj, rev_adj

    # ---------------------------------------------------------
    # 2. IDENTIFIKASI START & END EVENT
    # ---------------------------------------------------------
    def _find_start_and_end_nodes(self, nodes):
        start_nodes = []
        end_nodes = []
        for node_id, data in nodes.items():
            node_type = (data.get("type") or "").lower()
            if "start" in node_type:
                start_nodes.append(node_id)
            if "end" in node_type:
                end_nodes.append(node_id)
        return start_nodes, end_nodes

    # ---------------------------------------------------------
    # 3. BFS DARI START (REACHABLE FROM START)
    # ---------------------------------------------------------
    def _reachable_from_starts(self, start_nodes, adj):
        """BFS dari semua start event untuk mencari node yang reachable."""
        reachable = set()
        q = deque(start_nodes)
        while q:
            u = q.popleft()
            if u in reachable:
                continue
            reachable.add(u)
            for v in adj.get(u, []):
                if v not in reachable:
                    q.append(v)
        return reachable

    # ---------------------------------------------------------
    # 4. BFS TERBALIK DARI END (CAN REACH END)
    # ---------------------------------------------------------
    def _can_reach_end(self, end_nodes, rev_adj):
        """
        Reverse BFS dari semua end event untuk mencari node yang
        masih bisa mencapai end.
        """
        can_reach = set()
        q = deque(end_nodes)
        while q:
            u = q.popleft()
            if u in can_reach:
                continue
            can_reach.add(u)
            for v in rev_adj.get(u, []):
                if v not in can_reach:
                    q.append(v)
        return can_reach

    # ---------------------------------------------------------
    # 5. TARJAN SCC (CARI SIKLUS DALAM GRAPH)
    # ---------------------------------------------------------
    def _tarjan_scc(self, adj):
        """
        Tarjan's algorithm untuk mencari strongly connected components (SCC).
        Return list of components, masing2 berupa set node_id.
        """
        index = 0
        stack = []
        on_stack = set()
        indices = {}
        lowlink = {}
        sccs = []

        def strongconnect(v):
            nonlocal index
            indices[v] = index
            lowlink[v] = index
            index += 1
            stack.append(v)
            on_stack.add(v)

            for w in adj.get(v, []):
                if w not in indices:
                    strongconnect(w)
                    lowlink[v] = min(lowlink[v], lowlink[w])
                elif w in on_stack:
                    lowlink[v] = min(lowlink[v], indices[w])

            # Jika v adalah root SCC
            if lowlink[v] == indices[v]:
                scc = set()
                while True:
                    w = stack.pop()
                    on_stack.remove(w)
                    scc.add(w)
                    if w == v:
                        break
                sccs.append(scc)

        for v in list(adj.keys()):
            if v not in indices:
                strongconnect(v)

        return sccs

    # ---------------------------------------------------------
    # 6. AMBIL CONTOH JALUR LOOP DALAM SCC
    # ---------------------------------------------------------
    def _extract_cycle_example(self, component, adj):
        """
        Coba ambil satu contoh jalur loop di dalam SCC dengan DFS.
        Return list node_id yang membentuk cycle (termasuk node awal di akhir), atau None.
        """
        component = set(component)
        start = next(iter(component))

        path = []
        visited = set()

        def dfs(u):
            visited.add(u)
            path.append(u)

            for v in adj.get(u, []):
                if v not in component:
                    continue
                if v in path:
                    # ditemukan cycle: slice path-nya
                    idx = path.index(v)
                    return path[idx:] + [v]
                if v not in visited:
                    cycle = dfs(v)
                    if cycle:
                        return cycle

            path.pop()
            return None

        return dfs(start)

    # ---------------------------------------------------------
    # 7. DETEKSI LOOP & LOOP DEADLOCK
    # ---------------------------------------------------------
    def detect_loop_deadlocks(self, process_id):
        """
        Deteksi loop dalam process_id tertentu dan klasifikasikan:
        - loop_without_exit (tidak bisa mencapai End)  → kandidat loop deadlock
        - loop_with_exit   (masih bisa mencapai End)   → loop tapi bukan deadlock

        Logika:
        - Ambil SCC (cycle) yang:
          * reachable dari minimal satu start event (pakai BFS dari start).
        - Untuk tiap SCC, cek:
          * apakah ada node di SCC yang bisa mencapai End?
            - kalau TIDAK → loop_without_exit
            - kalau IYA  → loop_with_exit
        """
        nodes, adj, rev_adj = self._load_process_graph(process_id)
        start_nodes, end_nodes = self._find_start_and_end_nodes(nodes)

        if not start_nodes:
            print("Warning: tidak ada start event yang ditemukan")
        if not end_nodes:
            print("Warning: tidak ada end event yang ditemukan")

        # BFS dari start
        reachable = self._reachable_from_starts(start_nodes, adj) if start_nodes else set(nodes.keys())
        # BFS terbalik dari end
        can_reach_end = self._can_reach_end(end_nodes, rev_adj) if end_nodes else set()

        # Cari semua SCC
        sccs = self._tarjan_scc(adj)

        loops = []
        for comp in sccs:
            # Abaikan SCC trivial tanpa self-loop
            if len(comp) == 1:
                node = next(iter(comp))
                if node not in adj or node not in adj[node]:
                    continue

            # Harus ada yang reachable dari start
            if not (comp & reachable):
                continue

            # cek apakah ada anggota SCC yang bisa reach End
            reaches_end = bool(comp & can_reach_end)

            classification = "loop_without_exit" if not reaches_end else "loop_with_exit"

            cycle_example = self._extract_cycle_example(comp, adj)
            loops.append({
                "nodes": list(comp),
                "cycle_example": cycle_example,
                "node_details": {nid: nodes[nid] for nid in comp},
                "reaches_end": reaches_end,
                "classification": classification,
            })

        return loops


# ---------------------------------------------------------
# 8. MAIN – OUTPUT LEBIH JELAS
# ---------------------------------------------------------
if __name__ == "__main__":
    # --- KONFIGURASI NEO4J ---
    NEO4J_URI = "bolt://localhost:7687"
    NEO4J_USER = "neo4j"
    NEO4J_PASSWORD = "12345678"

    # Ganti dengan process_id BPMN yang ingin dicek
    PROCESS_ID = "dabc4f5c-1aea-4f27-a106-18930d60b904"

    detector = BPMNDeadlockDetector(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    try:
        loops = detector.detect_loop_deadlocks(PROCESS_ID)

        if not loops:
            print(f"Tidak ditemukan loop pada process_id = {PROCESS_ID}.")
        else:
            deadlock_like = [l for l in loops if l["classification"] == "loop_without_exit"]
            exitable     = [l for l in loops if l["classification"] == "loop_with_exit"]

            print(f"Ditemukan {len(loops)} loop pada process_id = {PROCESS_ID}.")
            print(f"  • {len(deadlock_like)} loop_without_exit (kandidat loop deadlock)")
            print(f"  • {len(exitable)} loop_with_exit (loop yang masih punya jalur ke End)\n")

            for i, dl in enumerate(loops, start=1):
                print("====================================================")
                print(f"[Loop #{i}]  klasifikasi = {dl['classification']}")
                if dl["reaches_end"]:
                    print("  (Beberapa node di loop ini masih bisa mencapai End Event.)")
                else:
                    print("  (Tidak ada node di loop ini yang bisa mencapai End Event.)")

                comp_nodes = dl["nodes"]
                node_details = dl["node_details"]

                print("\nNode yang terlibat dalam loop:")
                for nid in comp_nodes:
                    info = node_details.get(nid, {})
                    print(f"  - {nid}: type={info.get('type')}, labels={info.get('labels')}")

                if dl["cycle_example"]:
                    path_ids = dl["cycle_example"]
                    print("\nContoh jalur loop (urutan node id):")
                    print("  " + " -> ".join(path_ids))

                print("\nCatatan:")
                if dl["classification"] == "loop_without_exit":
                    print("  Loop ini tidak punya jalur keluar ke End, sehingga jika token masuk,")
                    print("  proses bisa berputar terus dan tidak pernah selesai (loop deadlock).")
                else:
                    print("  Loop ini masih memiliki jalur keluar ke End. Secara struktural ini")
                    print("  tetap loop, tetapi tidak otomatis deadlock; perilakunya tergantung")
                    print("  kondisi/gateway di model BPMN.\n")

    finally:
        detector.close()
