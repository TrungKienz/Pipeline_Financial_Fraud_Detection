# Tài Liệu Thiết Kế Pipeline Phát Hiện Gian Lận Thời Gian Thực

## 1. Mục tiêu

Hệ thống được thiết kế để đáp ứng bài toán:

> Real-time Data Integration & Stream Processing  
> ingest dữ liệu tốc độ cao từ nhiều nguồn, làm sạch và enrich theo thời gian thực, phát hiện bất thường ngay lập tức, hỗ trợ windowing, đảm bảo tính toàn vẹn dữ liệu và đo hiệu năng dưới nhiều mức tải.

Chủ đề áp dụng:

- `Real-time Fraud Detection`

## 2. Bối cảnh dữ liệu

Nguồn dữ liệu gốc:

- `F:\Project\Bigdata\Data\archive (2)\PS_20174392719_1491204439457_log.csv`

Các cột chính:

- `step`
- `type`
- `amount`
- `nameOrig`
- `oldbalanceOrg`
- `newbalanceOrig`
- `nameDest`
- `oldbalanceDest`
- `newbalanceDest`
- `isFraud`
- `isFlaggedFraud`

PaySim ban đầu là một bảng tích hợp sẵn. Tuy nhiên, để phù hợp với học phần tích hợp dữ liệu lớn, hệ thống hiện tại không xử lí trực tiếp theo kiểu một nguồn raw duy nhất đi vào Spark. Thay vào đó, bảng gốc được tách thành ba nguồn dữ liệu vật lý độc lập trước khi đưa lên Kafka.

## 3. Mô hình nhiều nguồn dữ liệu

### 3.1 Ba nguồn vật lý

Từ một bản ghi PaySim gốc, hệ thống sinh ra ba file CSV nguồn:

1. `transaction_source.csv`
2. `sender_state_source.csv`
3. `receiver_state_source.csv`

Ý nghĩa:

- `transaction_source.csv`: luồng giao dịch
- `sender_state_source.csv`: luồng trạng thái tài khoản bên gửi
- `receiver_state_source.csv`: luồng trạng thái tài khoản bên nhận

### 3.2 Vì sao phải tách thành 3 nguồn

Lý do thiết kế:

- mô phỏng đúng bài toán tích hợp nhiều nguồn dữ liệu
- tách phần ingestion ra khỏi bảng integrated gốc
- cho phép biểu diễn nhiều producer độc lập
- buộc Spark xử lí bài toán join và kiểm tra tính nhất quán khi tích hợp lại

## 4. Kiến trúc tổng thể

```text
PaySim CSV gốc
-> split_logical_sources.py
   -> transaction_source.csv
   -> sender_state_source.csv
   -> receiver_state_source.csv
-> 3 producer độc lập
   -> transaction_topic
   -> sender_state_topic
   -> receiver_state_topic
-> Spark Structured Streaming
   -> parse + validate
   -> dead-letter invalid records
   -> stream-stream join
   -> fraud scoring
   -> tumbling/sliding windows
-> sinks
   -> Cassandra
   -> Redis
   -> Kafka fraud_alerts
   -> Kafka metrics_windowed
```

## 5. Thành phần công nghệ

### 5.1 Kafka

Vai trò:

- nhận dữ liệu từ 3 producer độc lập
- làm message bus trung tâm
- hỗ trợ replay và benchmark throughput

Cấu hình hiện tại:

- Kafka `KRaft`
- topic nguồn: `transaction_topic`, `sender_state_topic`, `receiver_state_topic`
- topic nghiệp vụ: `risk_rules`, `fraud_alerts`, `metrics_windowed`, `pipeline_dead_letter`

### 5.2 Spark Structured Streaming

Vai trò:

- đọc 3 stream từ Kafka
- parse schema riêng cho từng nguồn
- phát hiện record lỗi và đẩy vào dead-letter
- join lại 3 luồng thành bản ghi tích hợp
- chạy fraud scoring
- tính tumbling window và sliding window metrics

### 5.3 Cassandra

Vai trò:

- lưu giao dịch đã xử lí
- lưu alert
- lưu metrics theo window
- lưu lịch sử account state
- lưu batch đã xử lí để chống replay

### 5.4 Redis

Vai trò:

- cache alert nóng
- lưu sender history phục vụ luật `rapid_outflow_pattern`
- hỗ trợ low-latency path

### 5.5 Streamlit

Vai trò:

- hiển thị dashboard demo
- đọc từ Cassandra và Redis

## 6. Thiết kế topic Kafka

### 6.1 `transaction_topic`

Schema logic:

- `event_id`
- `event_time`
- `producer_ts`
- `step`
- `type`
- `amount`
- `nameOrig`
- `nameDest`
- `isFraud`
- `isFlaggedFraud`
- `schema_version`

### 6.2 `sender_state_topic`

Schema logic:

- `event_id`
- `source_event_id`
- `event_time`
- `step`
- `nameOrig`
- `oldbalanceOrg`
- `newbalanceOrig`

### 6.3 `receiver_state_topic`

Schema logic:

- `event_id`
- `source_event_id`
- `event_time`
- `step`
- `nameDest`
- `oldbalanceDest`
- `newbalanceDest`

### 6.4 `risk_rules`

Dùng để cập nhật rule runtime, gồm các loại:

- `amount_threshold`
- `velocity_threshold`
- `watchlist_update`

### 6.5 `fraud_alerts`

Chứa alert sau khi Spark đánh giá giao dịch.

### 6.6 `metrics_windowed`

Chứa thống kê theo cửa sổ tumbling và sliding.

### 6.7 `pipeline_dead_letter`

Chứa các record lỗi parse, lỗi schema, mismatch semantics và cả bản ghi bị mồ côi khi không ghép đủ 3 nguồn.

Các nhóm lỗi chính:

- lỗi parse JSON hoặc thiếu trường bắt buộc
- sender hoặc receiver không ghép được với transaction trong khoảng join tolerance
- sender_state hoặc receiver_state trở thành orphan record
- mismatch khóa nghiệp vụ sau khi đã join theo `event_id`

## 7. Thiết kế khóa tích hợp dữ liệu

### 7.1 Khóa tương quan

Hệ thống sử dụng:

- `event_id` cho bản ghi transaction
- `source_event_id` cho sender và receiver state

Đây là khóa tương quan chính để join lại ba luồng.

### 7.2 Khóa nghiệp vụ bổ sung

Ngoài `event_id/source_event_id`, Spark còn kiểm tra thêm:

- `step`
- `nameOrig`
- `nameDest`
- `event_time`

Mục đích:

- tránh join nhầm khi dữ liệu bị lệch hoặc không nhất quán
- nâng độ tin cậy của phần tích hợp dữ liệu

## 8. Luồng xử lí chi tiết

### 8.1 Giai đoạn chuẩn bị nguồn

Script:

- [scripts/split_logical_sources.py](/f:/Project/Bigdata/scripts/split_logical_sources.py)

Nhiệm vụ:

- đọc PaySim gốc theo iterator
- tạo 3 CSV nguồn vật lý
- giữ quan hệ 1-1 giữa transaction, sender state và receiver state

### 8.2 Giai đoạn ingestion

Các producer:

- [scripts/publish_transaction_source.py](/f:/Project/Bigdata/scripts/publish_transaction_source.py)
- [scripts/publish_sender_state_source.py](/f:/Project/Bigdata/scripts/publish_sender_state_source.py)
- [scripts/publish_receiver_state_source.py](/f:/Project/Bigdata/scripts/publish_receiver_state_source.py)

Script điều phối song song:

- [scripts/publish_logical_sources_parallel.py](/f:/Project/Bigdata/scripts/publish_logical_sources_parallel.py)

Đặc điểm:

- ba tiến trình độc lập
- mỗi tiến trình publish một nguồn
- cùng tốc độ logic `rate`
- có retry khi kết nối Kafka

### 8.3 Giai đoạn parse và kiểm tra dữ liệu trong Spark

Spark:

- đọc 3 topic nguồn
- parse theo 3 schema độc lập
- bản ghi lỗi được đẩy vào `pipeline_dead_letter`

### 8.4 Giai đoạn join và tái tích hợp

Spark join:

- `transaction_topic` với `sender_state_topic`
- sau đó join với `receiver_state_topic`

Điều kiện:

- `transaction.event_id = sender.source_event_id`
- `transaction.event_id = receiver.source_event_id`
- kiểm tra thêm `step`, `nameOrig`, `nameDest`, `event_time`

Kết quả:

- tạo lại một bản ghi tích hợp hoàn chỉnh để phục vụ fraud scoring
- nếu thiếu `sender_state` hoặc `receiver_state`, Spark không drop im lặng mà đẩy record vào `pipeline_dead_letter`
- nếu `sender_state` hoặc `receiver_state` đến mà không có transaction tương ứng, hệ thống cũng ghi nhận orphan vào `pipeline_dead_letter`

### 8.5 Giai đoạn phát hiện gian lận

Rule engine hiện có:

- `high_amount_transfer`
- `sender_balance_inconsistency`
- `receiver_balance_inconsistency`
- `flagged_fraud`
- `rapid_outflow_pattern`
- `watchlist_hit`

Đầu ra:

- alert Kafka vào `fraud_alerts`
- alert lưu Cassandra
- alert cache trong Redis

### 8.6 Giai đoạn windowing

Hệ thống hỗ trợ:

- tumbling window
- sliding window

Metric chính:

- `event_count`
- `fraud_count`
- `total_amount`
- `fraud_rate`

## 9. Exactly-once semantics ở mức ứng dụng

### 9.1 Kafka producer

Các producer bật:

- `acks=all`
- `enable_idempotence=true`
- `max_in_flight_requests_per_connection=1`

### 9.2 Spark deduplication và replay safety

Trong Spark:

- dùng watermark cho stream-stream join
- chống lặp bằng `event_id`
- lưu `processed_stream_batches` trong Cassandra
- gắn `run_id` theo checkpoint root để tách biệt các lần chạy khác nhau

`run_id` được lưu trong file `.pipeline_run_id` dưới thư mục checkpoint. Nếu xóa checkpoint rồi chạy lại, hệ thống sinh `run_id` mới nên không bị va chạm `batch_id` cũ trong bảng `processed_stream_batches`.

Điều này giúp:

- giảm duplicate khi replay micro-batch
- tránh ghi sink lặp nếu batch đã xử lí

### 9.3 Giới hạn hiện tại

Hệ thống hiện tại đạt exactly-once theo hướng:

- gần đúng ở mức ứng dụng
- replay-safe cho sink chính
- an toàn hơn khi test nhỏ trước rồi mới chạy full luồng

Đối với runtime rule:

- mặc định Spark chụp snapshot rule một lần khi job khởi động
- chỉ refresh lại nếu đặt biến môi trường `RISK_RULE_REFRESH_SECONDS > 0`

Mặc định này giúp kết quả ổn định hơn trong cùng một lần chạy và tránh việc mỗi micro-batch lại đọc toàn bộ topic `risk_rules`.

Đây chưa phải exactly-once phân tán tuyệt đối kiểu production-grade end-to-end transaction trên mọi sink.

## 10. Lưu trữ và quan sát

### 10.1 Bảng Cassandra

Các bảng chính:

- `transactions_by_day`
- `alerts_by_account`
- `metrics_by_window`
- `account_state_by_account`
- `rules_by_id`
- `processed_stream_batches`

### 10.2 Redis

Redis dùng cho:

- cache alert
- sender event history

### 10.3 UI

- Kafka UI: kiểm tra topic và message
- Spark Master UI: kiểm tra cluster
- Spark App UI: kiểm tra streaming queries
- Spark History UI: kiểm tra history
- Streamlit: trình diễn dashboard

## 11. Benchmark và đánh giá hiệu năng

### 11.1 Benchmark in-memory

Script:

- [scripts/benchmark_local_pipeline.py](/f:/Project/Bigdata/scripts/benchmark_local_pipeline.py)

Mục tiêu:

- đo throughput logic trước khi lên full streaming
- so sánh các profile tải nhỏ, vừa, lớn

### 11.2 Benchmark streaming

Chiến lược:

1. chạy stack local sạch
2. publish 3 nguồn với `max-events` và `rate` tăng dần
3. theo dõi Kafka UI, Spark UI, Cassandra và Streamlit

Mức tải đề xuất để báo cáo:

- `1K events/sec`
- `10K events/sec`
- `50K events/sec`

Với local machine, có thể bắt đầu nhỏ hơn để lấy xu hướng rồi ngoại suy hoặc mô tả giới hạn môi trường.

### 11.3 Chỉ số cần thu thập

- latency đầu-cuối
- throughput thực tế
- số lượng alert
- số record dead-letter
- CPU/RAM container

## 12. Đánh giá theo rubric

### 12.1 System Analysis & Design

Đáp ứng:

- có phát biểu bài toán rõ
- kiến trúc nhiều nguồn dữ liệu rõ ràng
- dùng đúng hệ sinh thái Big Data: Kafka, Spark, Cassandra, Redis

### 12.2 Technical Implementation

Đáp ứng:

- pipeline local chạy ổn định
- có Docker Compose
- có 3 producer độc lập
- có Spark integration path
- có test, smoke test, benchmark script

### 12.3 Optimization & Research Depth

Đáp ứng một phần:

- có retry producer
- có replay-safe batch marker
- có dead-letter
- có benchmark in-memory

Cần làm mạnh hơn nếu muốn điểm cao hơn:

- so sánh nhiều cấu hình `rate`, `partitions`, `shuffle partitions`
- ghi lại biểu đồ latency và throughput
- phân tích nghẽn hiệu năng

### 12.4 Evaluation & Testing

Đáp ứng:

- có unit test
- có smoke test
- có benchmark script
- có thể đo số liệu thực nghiệm trên local stack

## 13. Hạn chế hiện tại

- dữ liệu nhiều nguồn được mô phỏng từ một dataset gốc, không phải ba hệ thống nguồn thật sự ngoài đời
- benchmark hiện chủ yếu ở local machine
- chưa có ML model riêng, hiện tại là rule-based fraud detection
- Streamlit thiên về demo hơn là dashboard production

## 14. Hướng mở rộng

- thêm mô hình học máy cho phần fraud scoring
- thêm collector metrics chuyên biệt cho Prometheus
- so sánh nhiều chiến lược partition Kafka
- đánh giá ảnh hưởng của window size và slide size
- mở rộng benchmark trên máy mạnh hơn hoặc cluster thực

## 15. File liên quan

- [README.md](/f:/Project/Bigdata/README.md)
- [docker-compose.yml](/f:/Project/Bigdata/docker-compose.yml)
- [scripts/split_logical_sources.py](/f:/Project/Bigdata/scripts/split_logical_sources.py)
- [scripts/publish_logical_sources_parallel.py](/f:/Project/Bigdata/scripts/publish_logical_sources_parallel.py)
- [scripts/bootstrap_local_stack.py](/f:/Project/Bigdata/scripts/bootstrap_local_stack.py)
- [spark-app/stream_job.py](/f:/Project/Bigdata/spark-app/stream_job.py)
- [docs/LOCAL_STEP_BY_STEP_RUNBOOK.md](/f:/Project/Bigdata/docs/LOCAL_STEP_BY_STEP_RUNBOOK.md)
