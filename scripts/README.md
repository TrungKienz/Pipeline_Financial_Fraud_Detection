# Connectivity scripts

`check_remote_services.py` bay gio dung de test ket noi den stack Docker Compose local va co the dung lai cho remote mode neu can.

Local mode se check:

- Kafka `9092`
- Kafka UI `8085`
- Cassandra `9042`
- Redis `6379`
- Spark master RPC `7077`
- Spark UI `8080`, `8081`, `4040`
- Spark History UI `18080`
- Streamlit `8501`

Chay local:

```powershell
python .\scripts\check_remote_services.py
```

Xuat JSON:

```powershell
python .\scripts\check_remote_services.py --json-out .\service-report.json
```

Neu can kiem tra remote mode:

```powershell
python .\scripts\check_remote_services.py --mode remote --host 163.223.13.187
```
