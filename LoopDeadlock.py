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
        membentuk adjacency list (graph) + informasi jenis relasi.

        Return:
            nodes: dict[node_id] = {
                "labels": [...],
                "type": "...",
                "name": "..." (jika ada di Neo4j)
            }
            base_adj:     dict[node_id] = [successor node_ids]
            base_rev_adj: dict[node_id] = [predecessor node_ids]
            rel_type_map: dict[(src_id, dst_id)] = rel_type_str
                          (contoh: "SEQUENCE_FLOW", "EXCLUSIVEGATEWAY", ...)
        """
        nodes = {}
        base_adj = defaultdict(list)
        base_rev_adj = defaultdict(list)
        rel_type_map = {}

        with self.driver.session() as session:
            # Ambil semua node untuk process_id tsb
            result = session.run(
                """
                MATCH (n)
                WHERE n.process_id = $process_id
                RETURN n.id   AS id,
                       labels(n) AS labels,
                       n.type AS type,
                       n.name AS name
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

            # Ambil semua edge (sequence flow, gateway, dll) untuk process_id tsb
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
                    base_adj[src].append(dst)
                    base_rev_adj[dst].append(src)
                    rel_type_map[(src, dst)] = rel_type

        return nodes, base_adj, base_rev_adj, rel_type_map

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
    # 7. BANGUN SKENARIO BERDASARKAN EXCLUSIVE GATEWAY
    # ---------------------------------------------------------
    def _build_scenarios_with_xor(self, nodes, base_adj, base_rev_adj, rel_type_map):
        """
        Membangun beberapa 'skenario' eksekusi dengan cara:
        - Skenario dasar: semua cabang aktif (seperti graf biasa).
        - Untuk setiap node yang punya outgoing edge EXCLUSIVEGATEWAY:
            untuk setiap cabang XOR, dibuat skenario di mana hanya cabang itu
            yang diaktifkan (cabang XOR lain di node tersebut dimatikan).
        """
        scenarios = []

        # Skenario dasar (semua cabang aktif)
        scenarios.append({
            "type": "base",
            "description": "Semua cabang (termasuk semua pilihan exclusive gateway) dianggap mungkin diambil.",
            "adj": base_adj,
            "rev_adj": base_rev_adj,
            "xor_src_id": None,
            "xor_src_name": None,
            "xor_dst_id": None,
            "xor_dst_name": None,
        })

        # Cari node yang memiliki outgoing EXCLUSIVEGATEWAY
        xor_sources = set()
        for (src, dst), rtype in rel_type_map.items():
            if (rtype or "").upper() == "EXCLUSIVEGATEWAY":
                xor_sources.add(src)

        # Untuk tiap node XOR source, buat skenario per cabang XOR
        for src in xor_sources:
            # Cari semua cabang XOR dari src
            xor_outgoing = [
                dst for (s, dst), rtype in rel_type_map.items()
                if s == src and (rtype or "").upper() == "EXCLUSIVEGATEWAY"
            ]
            src_name = nodes.get(src, {}).get("name") or src

            for dst_chosen in xor_outgoing:
                dst_name = nodes.get(dst_chosen, {}).get("name") or dst_chosen

                # Bangun adjacency baru: sama seperti base, kecuali di node src
                adj_var = defaultdict(list)
                rev_adj_var = defaultdict(list)

                for s, neighbors in base_adj.items():
                    for d in neighbors:
                        rtype = (rel_type_map.get((s, d)) or "").upper()

                        # Kalau ini node XOR dan relasinya EXCLUSIVEGATEWAY,
                        # hanya pertahankan cabang yang dipilih (dst_chosen)
                        if s == src and rtype == "EXCLUSIVEGATEWAY" and d != dst_chosen:
                            continue

                        adj_var[s].append(d)
                        rev_adj_var[d].append(s)

                desc = (
                    "Skenario pilihan cabang EXCLUSIVE GATEWAY: "
                    f"dari '{src_name}' hanya cabang ke '{dst_name}' yang dianggap diambil."
                )

                scenarios.append({
                    "type": "xor_choice",
                    "description": desc,
                    "adj": adj_var,
                    "rev_adj": rev_adj_var,
                    "xor_src_id": src,
                    "xor_src_name": src_name,
                    "xor_dst_id": dst_chosen,
                    "xor_dst_name": dst_name,
                })

        return scenarios

    # ---------------------------------------------------------
    # 8. DETEKSI LOOP & LOOP DEADLOCK DENGAN SKENARIO XOR
    # ---------------------------------------------------------
    def detect_loop_deadlocks(self, process_id):
        """
        Deteksi loop dalam process_id tertentu dan klasifikasikan:
        - loop_without_exit (tidak bisa mencapai End)  → kandidat loop deadlock
        - loop_with_exit   (masih bisa mencapai End)   → loop tapi masih punya jalan keluar

        Berbeda dari versi sebelumnya, di sini kita:
        - Mencoba skenario dasar (semua cabang aktif),
        - PLUS skenario-skenario di mana setiap EXCLUSIVEGATEWAY dipaksa
          memilih satu cabang saja, sehingga kita bisa melihat beberapa
          kemungkinan loop deadlock yang muncul dari kombinasi pilihan ini.
        """
        nodes, base_adj, base_rev_adj, rel_type_map = self._load_process_graph(process_id)
        start_nodes, end_nodes = self._find_start_and_end_nodes(nodes)

        if not start_nodes:
            print("Peringatan: tidak ada Start Event yang ditemukan pada proses ini.")
        if not end_nodes:
            print("Peringatan: tidak ada End Event yang ditemukan pada proses ini.")

        # Bangun berbagai skenario berdasarkan pilihan cabang EXCLUSIVE GATEWAY
        scenarios = self._build_scenarios_with_xor(nodes, base_adj, base_rev_adj, rel_type_map)

        all_loops = []

        for scenario_index, sc in enumerate(scenarios, start=1):
            adj = sc["adj"]
            rev_adj = sc["rev_adj"]

            # BFS dari start
            reachable = (
                self._reachable_from_starts(start_nodes, adj)
                if start_nodes else set(nodes.keys())
            )
            # BFS terbalik dari end
            can_reach_end = (
                self._can_reach_end(end_nodes, rev_adj)
                if end_nodes else set()
            )

            # Cari semua SCC (kandidat loop) pada skenario ini
            sccs = self._tarjan_scc(adj)

            for comp in sccs:
                # Abaikan SCC trivial tanpa self-loop
                if len(comp) == 1:
                    node = next(iter(comp))
                    if node not in adj or node not in adj[node]:
                        continue

                # Harus ada node yang reachable dari start
                if not (comp & reachable):
                    continue

                # cek apakah ada anggota SCC yang bisa reach End
                reaches_end = bool(comp & can_reach_end)
                classification = "loop_without_exit" if not reaches_end else "loop_with_exit"

                cycle_example = self._extract_cycle_example(comp, adj)

                all_loops.append({
                    "nodes": list(comp),
                    "cycle_example": cycle_example,
                    "node_details": {nid: nodes[nid] for nid in comp},
                    "reaches_end": reaches_end,
                    "classification": classification,
                    "scenario": sc,
                    "scenario_index": scenario_index,
                })

        return all_loops


# ---------------------------------------------------------
# 9. MAIN – OUTPUT LEBIH MUDAH DIPAHAMI ORANG AWAM + SKENARIO XOR
# ---------------------------------------------------------
if __name__ == "__main__":
    # --- KONFIGURASI NEO4J ---
    NEO4J_URI = "bolt://localhost:7687"
    NEO4J_USER = "neo4j"
    NEO4J_PASSWORD = "12345678"

    # Ganti dengan process_id BPMN yang ingin dicek
    PROCESS_ID = "8e8851c7-a8d2-4953-8f03-18ebc6c5edbf"

    detector = BPMNDeadlockDetector(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    try:
        loops = detector.detect_loop_deadlocks(PROCESS_ID)

        if not loops:
            print(f"Tidak ditemukan bagian proses yang berulang (loop) pada process_id = {PROCESS_ID}.")
        else:
            deadlock_like = [l for l in loops if l["classification"] == "loop_without_exit"]
            exitable     = [l for l in loops if l["classification"] == "loop_with_exit"]

            print("================================================================")
            print(f"Hasil Analisis Loop pada proses: {PROCESS_ID}")
            print("================================================================")
            print(f"- Total kombinasi skenario yang dianalisis (base + XOR): "
                  f"{max(l['scenario_index'] for l in loops)}")
            print(f"- Total bagian proses yang membentuk pengulangan (loop) di semua skenario: {len(loops)}")
            print(f"  • {len(deadlock_like)} loop berisiko TIDAK memiliki jalan keluar ke akhir proses")
            print(f"  • {len(exitable)} loop masih memiliki jalur keluar ke akhir proses\n")

            for i, dl in enumerate(loops, start=1):
                comp_nodes     = dl["nodes"]
                node_details   = dl["node_details"]
                classification = dl["classification"]
                scenario       = dl["scenario"]
                scen_desc      = scenario["description"]

                # Judul jenis loop dalam bahasa awam
                if classification == "loop_without_exit":
                    jenis = "LOOP BERISIKO (tidak punya jalan keluar ke End)"
                else:
                    jenis = "LOOP DENGAN JALUR KELUAR (masih bisa ke End)"

                print("====================================================")
                print(f"[Loop #{i}] {jenis}")
                print(f"  Skenario: {scen_desc}\n")

                # Penjelasan singkat jenis loop
                if classification == "loop_without_exit":
                    print("  Artinya:")
                    print("    Dalam skenario pemilihan cabang ini, bagian proses di bawah")
                    print("    membentuk pengulangan tanpa jalur yang jelas untuk kembali")
                    print("    ke akhir proses. Jika alur masuk ke sini, proses berisiko")
                    print("    'berputar-putar' dan tidak pernah selesai.\n")
                else:
                    print("  Artinya:")
                    print("    Dalam skenario ini, bagian proses memang mengandung pengulangan,")
                    print("    tetapi masih ada jalur keluar yang mengarah ke End Event.\n")

                # Tampilkan node dalam loop (nama + jenis)
                print("  Aktivitas/gateway yang termasuk dalam loop ini:")
                for nid in comp_nodes:
                    info = node_details.get(nid, {})
                    nm   = info.get("name") or "(tanpa nama)"
                    tipe = info.get("type") or "unknown"
                    print(f"    - {nm}  [type={tipe}, id={nid}]")

                # Tampilkan contoh jalur loop dalam bentuk nama aktivitas
                if dl["cycle_example"]:
                    path_ids = dl["cycle_example"]
                    path_names = []
                    for nid in path_ids:
                        info = node_details.get(nid, {})
                        nm = info.get("name") or nid
                        path_names.append(nm)

                    print("\n  Contoh alur pengulangan (loop) di bagian ini:")
                    print(f"    { ' -> '.join(path_names) }")

                print("\n  Catatan tambahan:")
                if classification == "loop_without_exit":
                    print("    • Perlu ditinjau apakah memang diinginkan proses bisa berulang terus,")
                    print("      atau seharusnya ada kondisi keluar yang mengarah ke akhir proses.")
                else:
                    print("    • Loop ini tidak otomatis salah. Namun tetap perlu dicek apakah")
                    print("      kondisi keluar dari loop sudah sesuai dengan aturan bisnis.")
                print()

    finally:
        detector.close()
