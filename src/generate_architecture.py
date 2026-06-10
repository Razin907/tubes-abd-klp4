import os
from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.workflow import Airflow
from diagrams.onprem.database import Postgresql
from diagrams.onprem.inmemory import Redis
from diagrams.aws.storage import S3
from diagrams.onprem.queue import Celery
from diagrams.onprem.client import Client

# Tambahkan lokasi Graphviz ke PATH
os.environ["PATH"] += os.pathsep + r"C:\Program Files\Graphviz\bin"
os.makedirs("gambar", exist_ok=True)

graph_attr = {
    "fontsize": "24",
    "fontname": "Helvetica-Bold",
    "bgcolor": "white",
    "pad": "0.5",
    "splines": "spline",   # Polyline/spline agar label edge dapat dirender dengan benar
    "nodesep": "1.2",
    "ranksep": "1.5",
    "rankdir": "TB"
}

node_attr = {
    "fontname": "Helvetica-Bold",
    "fontsize": "14"
}

edge_attr = {
    "fontname": "Helvetica-Bold",
    "fontsize": "13"
}

with Diagram(
    show=False,
    filename="gambar/arsitektur_terdistribusi",
    outformat="png",
    graph_attr=graph_attr,
    node_attr=node_attr,
    edge_attr=edge_attr,
    direction="TB"  # Top-to-Bottom
):
    with Cluster("Pusat Kontrol"):
        flower = Client("Flower Dashboard\n(Monitoring)")
        airflow = Airflow("Airflow Webserver\n& Scheduler")

    with Cluster("Cloud Services (Publik)"):
        upstash = Redis("Upstash Redis\n(Broker Tugas)")
        supabase = Postgresql("Supabase PostgreSQL\n(Metadata)")

    with Cluster("Pekerja Paralel (Remote Workers)"):
        worker1 = Celery("Worker 1")
        worker2 = Celery("Worker 2")
        worker3 = Celery("Worker 3")

    with Cluster("Data Lake"):
        minio = S3("MinIO S3\n(Penyimpanan Akhir)")

    # ----------------------------------------------------
    # Alur Utama (Top-to-Bottom)
    # ----------------------------------------------------
    # Airflow -> Redis
    airflow >> Edge(color="#ea580c", label="  ① Kirim Tugas  ", style="bold", penwidth="2.5", fontcolor="#c2410c") >> upstash
    
    # Redis -> Workers
    upstash >> Edge(color="#2563eb", label="  ② Ambil Tugas  ", style="bold", penwidth="2.5", fontcolor="#1d4ed8") >> worker1
    upstash >> Edge(color="#2563eb", style="bold", penwidth="2.5") >> worker2
    upstash >> Edge(color="#2563eb", style="bold", penwidth="2.5") >> worker3
    
    # Workers -> Minio
    worker1 >> Edge(color="#7e22ce", label="  ④ Upload CSV  ", style="dashed", penwidth="2.0", fontcolor="#6b21a8") >> minio
    worker2 >> Edge(color="#7e22ce", style="dashed", penwidth="2.0") >> minio
    worker3 >> Edge(color="#7e22ce", style="dashed", penwidth="2.0") >> minio

    # ----------------------------------------------------
    # Alur Sekunder (Back-edges / Samping) -> constraint=false
    # ----------------------------------------------------
    # Workers -> Supabase (Lapor Status)
    worker1 >> Edge(color="#16a34a", label="  ③ Lapor Status  ", style="dashed", penwidth="2.0", fontcolor="#15803d", constraint="false") >> supabase
    worker2 >> Edge(color="#16a34a", style="dashed", penwidth="2.0", constraint="false") >> supabase
    worker3 >> Edge(color="#16a34a", style="dashed", penwidth="2.0", constraint="false") >> supabase

    # Airflow -> Supabase (Sinkron DB)
    airflow >> Edge(color="#64748b", label="  Sinkron DB  ", style="dotted", penwidth="1.5", fontcolor="#475569", constraint="false") >> supabase
    
    # Flower -> Redis (Pantau Worker)
    flower >> Edge(color="#dc2626", label="  Pantau Worker  ", style="dotted", penwidth="1.5", fontcolor="#b91c1c", constraint="false") >> upstash

print("Gambar berhasil dibuat. Masalah label overlapping dari 'ortho' telah diatasi menggunakan 'spline'.")