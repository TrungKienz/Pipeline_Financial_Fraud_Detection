# 🚀 HƯỚNG DẪN CHẠY FULL PIPELINE VỚI DỮ LIỆU THẬT (500MB)

Tài liệu này dành cho Senior Operator để vận hành toàn bộ luồng dữ liệu PaySim gốc (hơn 6 triệu bản ghi). Hãy thực hiện theo đúng thứ tự để đảm bảo tính nhất quán của dữ liệu.

---

## 🧹 BƯỚC 1: LÀM SẠCH HỆ THỐNG (TOTAL RESET)

Trước khi chạy dữ liệu lớn, bạn cần đảm bảo không có dữ liệu rác từ các lần test trước.

1.  **Dừng và xóa toàn bộ Database:**
    ```powershell
    docker-compose down -v
    ```

2.  **Xóa sạch Checkpoints của Spark:**
    ```powershell
    # Xóa thư mục lưu trạng thái của Spark để tránh xung đột Schema
    Remove-Item -Recurse -Force f:\Project\Bigdata\spark_checkpoints\*
    ```

---

## ✂️ BƯỚC 2: CHIA TÁCH DỮ LIỆU GỐC (SPLIT PHASE)

Chúng ta sẽ chia tệp PaySim 493MB thành 3 nguồn dữ liệu độc lập để mô phỏng hệ thống ngân hàng.

**Lệnh thực hiện:**
```powershell
python scripts/split_logical_sources.py --csv-path "Data\archive (2)\PS_20174392719_1491204439457_log.csv" --output-dir "Data\logical_sources"
```
*(Đợi khoảng 30-45 giây để script xử lý xong hàng triệu dòng dữ liệu)*

---

## 🏗️ BƯỚC 3: KHỞI ĐỘNG HẠ TẦNG

1.  **Bật các Container:**
    ```powershell
    docker-compose up -d
    ```

2.  **Khởi tạo Schema (Sau khi Cassandra báo Healthy):**
    ```powershell
    python scripts/bootstrap_local_stack.py
    ```

---

## 🌊 BƯỚC 4: BƠM DỮ LIỆU TOÀN PHẦN (FULL INGESTION)

Bây giờ là lúc bơm toàn bộ dữ liệu vào đường ống. Chúng ta sẽ bỏ tham số `--max-events` để script chạy cho đến hết file.

**Lệnh thực hiện (Chạy trong Terminal riêng):**
```powershell
python scripts/publish_logical_sources_parallel.py --rate 100
```
-   **--rate 100:** Đẩy 100 giao dịch/giây (Tương đương 300 tin nhắn Kafka/giây). Bạn có thể tăng lên nếu máy tính đủ mạnh.

---

## 🕵️ BƯỚC 5: KIỂM TRA KẾT QUẢ

1.  **Dashboard chính:** Mở `http://localhost:8501`. Quan sát cột **Rule Score** (Logic luật) và **ML Score** (Logic AI) để so sánh độ chính xác.
2.  **Grafana:** Mở `http://localhost:3001`. Quan sát biểu đồ **Throughput (EPS)** và **Avg Batch Duration (ms)**. Nếu Latency tăng cao > 5000ms, bạn nên cân nhắc giảm `--rate`.
3.  **Logs:** Kiểm tra xem Spark có bị quá tải không:
    ```powershell
    docker logs -f spark-fraud-detection
    ```

---

### ⚠️ LƯU Ý QUAN TRỌNG:
-   Dữ liệu 500MB có thể khiến Docker Desktop tiêu tốn nhiều CPU. Nếu thấy máy quá lag, hãy giảm `--rate` xuống `20`.
-   Đảm bảo ổ đĩa còn trống ít nhất **5GB** cho Cassandra và Kafka lưu trữ dữ liệu.
