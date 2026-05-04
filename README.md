PaySimTôi muốn làm như này (🔹 Thiết kế luồng dữ liệu và kiến trúc hệ thống
1. Phân tách dữ liệu
Dữ liệu từ tập PaySim được chia thành 3 bảng (logical streams):
(1) Transaction
step
type
amount
nameOrig (sender)
nameDest (receiver)
(2) Sender State
nameOrig
oldbalanceOrg
newbalanceOrig
step
(3) Receiver State
nameDest
oldbalanceDest
newbalanceDest
step

2. Hệ thống ingestion (Kafka)
Tạo 3 topic tương ứng trên Apache Kafka:
transaction_topic
sender_state_topic
receiver_state_topic
Cấu hình:
đảm bảo Exactly-once semantics (tránh mất hoặc trùng dữ liệu)
thiết lập partition để tăng khả năng xử lý song song

3. Xử lý dữ liệu (Stream Processing)
Sử dụng Apache Spark (Structured Streaming):
Đọc dữ liệu từ 3 Kafka topics
Thực hiện join các stream theo khóa (nameOrig, nameDest, step)
Xử lý theo hướng Hybrid:
Rule-based (luật cố định)
Machine Learning (mô hình học máy nếu có)

4. Phát hiện gian lận (Fraud Detection Pipeline)
Kết quả phát hiện gian lận được đẩy sang Redis:
phục vụ xử lý nhanh (real-time alert / caching)

5. Lưu trữ dữ liệu
Dữ liệu sau xử lý (fraud / non-fraud) được lưu vào
 Apache Cassandra:
phục vụ lưu trữ lâu dài
hỗ trợ truy vấn phân tán quy mô lớn

6. Visualization & Alerting
Sử dụng Grafana:
hiển thị dashboard (transaction, fraud rate, etc.)
trực quan hóa pipeline dữ liệu
thiết lập cảnh báo (alert) theo thời gian thực

7. Monitoring & Benchmarking
Sử dụng Prometheus:
thu thập metrics hệ thống:
throughput (events/sec)
latency
CPU / RAM usage
Thực hiện benchmark:
(đo số lượng sự kiện xử lý trong 1 phút,đánh giá hiệu năng hệ thống dưới các mức tải khác nhau)
test multiple loads:
1K events/sec
10K events/sec
50K events/sec
measure:
latency
throughput ) 



