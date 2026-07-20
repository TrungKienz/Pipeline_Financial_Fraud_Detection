# Hướng Dẫn Vận Hành Hệ Thống Phát Hiện Gian Lận Real-time (PaySim)

Tài liệu này hướng dẫn cách chạy, giám sát và kiểm thử hệ thống tích hợp dữ liệu PaySim.

## 1. Yêu cầu hệ thống
- Docker & Docker Compose (tối thiểu 8GB RAM cho cụm stack).
- Python 3.9+ (để chạy các script test).
- Kafka, Spark, Cassandra, Redis (tất cả chạy trong Docker).

## 2. Khởi động hạ tầng (Infrastructure)
Trước khi làm bất cứ việc gì, bạn cần bật các dịch vụ Docker:

```powershell
docker-compose up -d
```
Đợi khoảng 1-2 phút để Kafka và Cassandra khởi động xong.

## 3. Nạp dữ liệu ban đầu (Bootstrap Data)
Chạy script sau để tự động phân tách dữ liệu PaySim và nạp các luật rủi ro cùng 50 giao dịch mẫu vào Kafka:

```powershell
python scripts/bootstrap_local_stack.py
```
Lệnh này sẽ thực hiện:
1. Khởi tạo Schema trong Cassandra (Tables).
2. Đẩy các Risk Rules vào topic `risk_rules`.
3. Thiết lập môi trường sạch (mặc định không nạp dữ liệu mẫu để sẵn sàng cho Full Ingestion).

## 3. Chạy Pipeline Xử lý (Spark Streaming)
Hệ thống sẽ tự động chạy thông qua service `spark-app` trong Docker. Để xem log:

```powershell
docker logs -f spark-fraud-detection
```

## 4. Thực hiện Tích hợp & Ingestion
Để mô phỏng việc tích hợp dữ liệu từ 3 nguồn độc lập:

```powershell
python scripts/publish_logical_sources_parallel.py --rate 100 --max-events 5000
```

## 5. Giám sát hệ thống (Observability)
Truy cập các địa chỉ sau trên trình duyệt:
- **Grafana (Monitoring Dashboard):** `http://localhost:3001` (User: `admin`, Pass: `admin`).
- **Streamlit (Business Dashboard):** `http://localhost:8501`.
- **Kafka UI:** `http://localhost:8085`.
- **Spark Master UI:** `http://localhost:8080`.

## 6. Kiểm thử hiệu năng (Benchmarking)
Để đo khả năng chịu tải của hệ thống (Ví dụ: 10,000 sự kiện/giây):

```powershell
python scripts/stress_test_pipeline.py --eps 10000 --duration 60
```

## 7. Các Case xử lý dữ liệu đặc biệt
Hệ thống đã được thiết kế để xử lý:
- **Late Data:** Sử dụng Watermark 10 phút.
- **Out-of-order:** Join tolerance 30 giây giữa các luồng.
- **Data Quality:** Tự động lọc các bản ghi có số tiền âm hoặc thiếu trường quan trọng vào `pipeline_dead_letter`.
- **Hybrid Scoring:** Kết hợp Rule-based và ML Model. Kết quả hiển thị song song trên Dashboard để đối soát.
- **Premium UI:** Giao diện Glassmorphism hiện đại, hỗ trợ giám sát thời gian thực với độ trễ cực thấp.

## 8. Xử lý lỗi (Troubleshooting)
- Nếu Spark không nhận dữ liệu: Kiểm tra topic Kafka bằng Kafka UI.
- Nếu Cassandra không lưu được: Kiểm tra logs `docker logs cassandra`.
- Xem các bản ghi lỗi: Truy cập Kafka topic `pipeline_dead_letter`.
