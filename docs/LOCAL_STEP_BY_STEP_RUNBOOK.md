# Hướng Dẫn Chạy Từng Bước Trên Máy Local

Tài liệu này hướng dẫn bạn chạy dự án theo đúng luồng hiện tại của repo:

`PaySim gốc -> tách 3 nguồn dữ liệu vật lý -> publish 3 topic Kafka -> Spark join lại -> phát hiện fraud -> lưu Cassandra/Redis -> quan sát qua UI`

Tài liệu được viết theo kiểu thao tác từng bước. Ở mỗi bước đều có:

- mục tiêu
- lệnh cần chạy
- kết quả mong đợi
- lỗi thường gặp và cách xử lí ngắn

## 1. Mục tiêu của runbook

Runbook này dùng để:

- kiểm tra logic cục bộ trước khi chạy full stack
- chạy toàn bộ hệ thống trên máy local
- xác nhận bài toán tích hợp 3 nguồn dữ liệu là đúng bản chất
- kiểm tra Spark streaming xử lí đúng khi đủ nguồn, thiếu nguồn, lệch khóa tích hợp
- chuẩn bị cho benchmark và báo cáo đồ án

## 2. Điều kiện cần có

Bạn cần:

- Windows + PowerShell
- Docker Desktop
- Python
- dữ liệu PaySim gốc

Dataset mặc định mà code đang dùng:

- `F:\Project\Bigdata\Data\archive (2)\PS_20174392719_1491204439457_log.csv`

Nếu dữ liệu của bạn ở vị trí khác, các script đều cho phép truyền lại `--csv-path`.

## 3. Cấu trúc hệ thống bạn sẽ chạy

Các thành phần chính:

- Kafka KRaft
- Kafka UI
- Spark Master
- Spark Worker
- Spark History Server
- Spark app `spark-fraud-detection`
- Cassandra
- Redis
- Streamlit dashboard

Các topic Kafka chính:

- `transaction_topic`
- `sender_state_topic`
- `receiver_state_topic`
- `risk_rules`
- `fraud_alerts`
- `metrics_windowed`
- `pipeline_dead_letter`

## 4. Thứ tự chạy khuyến nghị

Bạn nên chạy theo thứ tự này:

1. đứng đúng thư mục dự án
2. cài dependency local
3. chạy unit test
4. chạy smoke test nhỏ
5. chạy benchmark in-memory nhỏ
6. reset Docker stack
7. khởi động Docker stack
8. kiểm tra health service
9. tách dữ liệu thành 3 CSV nguồn
10. publish `risk_rules`
11. publish 3 nguồn dữ liệu lên Kafka
12. kiểm tra UI và dữ liệu sink
13. chạy validation end-to-end cho luồng tích hợp
14. tăng tải dần để benchmark

Nếu bạn chỉ muốn test nhanh end-to-end, có thể dùng script bootstrap ở phần sau. Tuy nhiên vẫn nên đi qua các bước nhỏ ít nhất một lần.

## 5. Step 0: Đứng đúng thư mục dự án

Mục tiêu:

- tránh chạy sai đường dẫn
- đảm bảo các script dùng đúng relative path

Lệnh:

```powershell
cd F:\Project\Bigdata
```

Kết quả mong đợi:

- terminal đang đứng trong thư mục `F:\Project\Bigdata`

## 6. Step 1: Cài dependency local

Mục tiêu:

- cài các thư viện cần cho script local
- chuẩn bị cho phần publish Kafka, Redis, Cassandra validation

Lệnh:

```powershell
python -m pip install -r requirements-local.txt
```

File dependency hiện tại gồm:

- `kafka-python`
- `redis`
- `cassandra-driver`

Kết quả mong đợi:

- không có lỗi import khi chạy script trong thư mục `scripts`

Lỗi thường gặp:

- `ModuleNotFoundError: No module named 'kafka'`
  Cách xử lí: chạy lại lệnh cài dependency ở trên

## 7. Step 2: Chạy unit test

Mục tiêu:

- xác nhận parser, rule engine, source split, validation helper vẫn đúng
- bắt lỗi logic trước khi lên Docker stack

Lệnh:

```powershell
python -m unittest discover -s tests -v
```

Kết quả mong đợi:

- tất cả test đều `ok`

Nếu bước này fail:

- chưa nên chạy full stack
- sửa lỗi logic trước, đặc biệt ở:
  `fraud_pipeline`
  `spark-app/stream_job.py`
  `scripts`

## 8. Step 3: Chạy smoke test nhỏ

Mục tiêu:

- kiểm tra dữ liệu PaySim đọc được
- kiểm tra rule engine sinh alert
- kiểm tra tumbling/sliding window hoạt động ở mức in-memory

Lệnh:

```powershell
python .\scripts\smoke_local_pipeline.py --limit 20
python .\scripts\smoke_local_pipeline.py --limit 200 --json-out .\smoke-summary.json
```

Kết quả mong đợi:

- script in ra JSON summary
- có `events_processed`
- có thể có `alerts_emitted`
- có danh sách `tumbling_windows` và `sliding_windows`

Khi nào dừng lại để sửa:

- parser không đọc được CSV
- `events_processed = 0`
- lỗi kiểu dữ liệu hoặc timestamp

## 9. Step 4: Chạy benchmark in-memory nhỏ

Mục tiêu:

- đo trước hiệu năng logic cục bộ
- không phụ thuộc Kafka/Spark/Cassandra
- lấy baseline trước khi benchmark streaming thật

Lệnh tối thiểu:

```powershell
python .\scripts\benchmark_local_pipeline.py --seed-limit 20 --profiles smoke=100
```

Ví dụ mở rộng:

```powershell
python .\scripts\benchmark_local_pipeline.py --seed-limit 100 --profiles small=500 medium=2000
python .\scripts\generate_benchmark_report.py --input .\benchmark-results.json --output .\benchmark-report.html
```

Kết quả mong đợi:

- có file `benchmark-results.json`
- có thể sinh `benchmark-report.html`

Lưu ý:

- đây chưa phải benchmark streaming end-to-end
- đây chỉ là benchmark logic trong bộ nhớ

## 10. Step 5: Reset Docker stack local

Mục tiêu:

- xóa state cũ của Kafka, Cassandra, Redis, Spark checkpoints
- tránh dữ liệu cũ làm nhiễu test mới

Lệnh:

```powershell
docker compose down -v
```

Kết quả mong đợi:

- các container dừng
- volume của stack local bị xóa

Lưu ý quan trọng:

- lệnh này xóa dữ liệu cũ
- nếu trước đó Spark đã tạo checkpoint, lần chạy mới sẽ tạo `run_id` mới
- nhờ vậy không bị đụng `batch_id` cũ trong bảng `processed_stream_batches`

## 11. Step 6: Khởi động Docker stack

Mục tiêu:

- khởi động toàn bộ Kafka, Spark, Cassandra, Redis, Streamlit

Lệnh:

```powershell
docker compose up -d
docker compose ps
```

Kết quả mong đợi:

- các service chính đều `Up`
- `kafka-init` và `cassandra-init` có thể ở trạng thái `Exited` nhưng phải là completed thành công

Các service chính bạn cần nhìn thấy:

- `kafka`
- `kafka-ui`
- `cassandra`
- `redis`
- `spark-master`
- `spark-worker`
- `spark-history`
- `spark-fraud-detection`
- `streamlit`

Lưu ý cập nhật:

- `streamlit` hiện được build sẵn từ [dashboard/streamlit/Dockerfile](/f:/Project/Bigdata/dashboard/streamlit/Dockerfile)
- không còn cài package lúc container khởi động nên thời gian warm-up ổn định hơn

## 12. Step 7: Kiểm tra health service

Mục tiêu:

- xác nhận các cổng và service đã sẵn sàng

Lệnh:

```powershell
python .\scripts\check_remote_services.py
python .\scripts\check_remote_services.py --json-out .\service-report.json
```

Kết quả mong đợi:

- Kafka reachable
- Cassandra reachable
- Redis ping thành công
- Spark UI và Streamlit trả HTTP `200`
- file [service-report.json](/f:/Project/Bigdata/service-report.json) báo tất cả service `ok`

Các endpoint local hiện tại:

- Kafka UI: `http://localhost:8085`
- Spark Master UI: `http://localhost:8080`
- Spark Worker UI: `http://localhost:8081`
- Spark App UI: `http://localhost:4040`
- Spark History UI: `http://localhost:18080`
- Streamlit: `http://localhost:8501`

Lưu ý:

- script healthcheck hiện đã có retry HTTP để giảm false negative khi `kafka-ui` hoặc `streamlit` vừa mới lên

Nếu còn lỗi:

- xem log container ở bước tiếp theo

## 13. Step 8: Xem log khi có lỗi

Mục tiêu:

- xác định lỗi do Kafka, Cassandra hay Spark app

Lệnh:

```powershell
docker compose logs --tail=200 spark-app
docker compose logs --tail=200 kafka
docker compose logs --tail=200 cassandra
docker compose logs --tail=200 streamlit
```

Nếu muốn theo dõi liên tục:

```powershell
docker compose logs -f spark-app
```

Dấu hiệu tốt:

- Spark app chạy ổn định
- không có lỗi kết nối Kafka/Cassandra kéo dài
- không có container restart liên tục
- `streamlit` log có dòng `You can now view your Streamlit app in your browser.`

## 14. Step 9: Tách dữ liệu gốc thành 3 CSV nguồn vật lý

Mục tiêu:

- biến một CSV PaySim gốc thành 3 nguồn dữ liệu độc lập
- đúng tinh thần bài toán tích hợp dữ liệu nhiều nguồn

Lệnh tối thiểu:

```powershell
python .\scripts\split_logical_sources.py --max-events 100
```

Ví dụ nếu file CSV gốc nằm nơi khác:

```powershell
python .\scripts\split_logical_sources.py --csv-path D:\data\paysim.csv --output-dir .\Data\logical_sources --max-events 100
```

Kết quả mong đợi:

- sinh ra 3 file:
  [transaction_source.csv](/f:/Project/Bigdata/Data/logical_sources/transaction_source.csv)
  [sender_state_source.csv](/f:/Project/Bigdata/Data/logical_sources/sender_state_source.csv)
  [receiver_state_source.csv](/f:/Project/Bigdata/Data/logical_sources/receiver_state_source.csv)

Ý nghĩa từng file:

- `transaction_source.csv`: giao dịch gốc
- `sender_state_source.csv`: trạng thái tài khoản bên gửi
- `receiver_state_source.csv`: trạng thái tài khoản bên nhận

Kiểm tra nhanh:

- số dòng của 3 file phải tương ứng 1-1 theo cùng một `event_id/source_event_id`

## 15. Step 10: Publish `risk_rules`

Mục tiêu:

- nạp bộ rule runtime vào Kafka để Spark dùng khi chấm điểm giao dịch

Lệnh:

```powershell
python .\scripts\publish_risk_rules.py
```

Topic:

- `risk_rules`

Kết quả mong đợi:

- topic `risk_rules` có message

Lưu ý:

- mặc định Spark chụp snapshot rule lúc job khởi động
- nếu muốn refresh rule định kỳ khi job đang chạy, đặt biến môi trường `RISK_RULE_REFRESH_SECONDS`

## 16. Step 11: Publish 3 nguồn dữ liệu lên Kafka

Mục tiêu:

- mô phỏng 3 nguồn dữ liệu độc lập
- để Spark đọc 3 topic và join lại

Khuyến nghị dùng chế độ song song:

```powershell
python .\scripts\publish_logical_sources_parallel.py --source-dir .\Data\logical_sources --max-events 100 --rate 20
```

Script này gọi 3 producer độc lập:

- [publish_transaction_source.py](/f:/Project/Bigdata/scripts/publish_transaction_source.py)
- [publish_sender_state_source.py](/f:/Project/Bigdata/scripts/publish_sender_state_source.py)
- [publish_receiver_state_source.py](/f:/Project/Bigdata/scripts/publish_receiver_state_source.py)

Nếu muốn chạy thủ công từng nguồn:

```powershell
python .\scripts\publish_transaction_source.py --source-dir .\Data\logical_sources --max-events 100 --rate 20
python .\scripts\publish_sender_state_source.py --source-dir .\Data\logical_sources --max-events 100 --rate 20
python .\scripts\publish_receiver_state_source.py --source-dir .\Data\logical_sources --max-events 100 --rate 20
```

Giải thích tham số:

- `--max-events`: số event muốn bơm
- `--rate`: số event logic mỗi giây

Khuyến nghị ban đầu:

- test nhỏ: `--max-events 100 --rate 20`
- test vừa: `--max-events 500 --rate 50`

## 17. Step 12: Nếu muốn chạy bootstrap bằng một lệnh

Mục tiêu:

- gom các bước tách dữ liệu + publish rules + publish 3 nguồn

Lệnh:

```powershell
python .\scripts\bootstrap_local_stack.py --max-events 100 --rate 20
```

Script này sẽ:

1. tách dữ liệu PaySim gốc thành 3 CSV nguồn
2. publish `risk_rules`
3. chạy 3 producer độc lập theo chế độ `parallel`

Nếu muốn ép dùng cách publish gộp:

```powershell
python .\scripts\bootstrap_local_stack.py --max-events 100 --rate 20 --producer-mode combined
```

Khuyến nghị:

- lần đầu nên chạy thủ công từng bước
- những lần sau có thể dùng bootstrap để tiết kiệm thời gian

## 18. Step 13: Kiểm tra Kafka UI

Mục tiêu:

- xác nhận message đã lên topic
- kiểm tra DLQ có hoạt động hay không

Mở:

- `http://localhost:8085`

Các topic cần kiểm tra:

- `transaction_topic`
- `sender_state_topic`
- `receiver_state_topic`
- `risk_rules`
- `fraud_alerts`
- `metrics_windowed`
- `pipeline_dead_letter`

Kết quả mong đợi:

- 3 topic nguồn có message
- `risk_rules` có message
- `fraud_alerts` có thể có hoặc chưa có tùy mẫu dữ liệu
- `metrics_windowed` có message sau khi Spark xử lí
- `pipeline_dead_letter` nên rỗng nếu dữ liệu sạch

Nếu `pipeline_dead_letter` có dữ liệu:

- cần đọc lỗi bên trong để biết là lỗi parse, thiếu nguồn, orphan hay mismatch semantics

## 19. Step 14: Kiểm tra Spark UI

Mục tiêu:

- xác nhận Spark app đang chạy thật
- theo dõi query streaming

Các UI:

- Spark Master UI: `http://localhost:8080`
- Spark Worker UI: `http://localhost:8081`
- Spark App UI: `http://localhost:4040`
- Spark History UI: `http://localhost:18080`

Kết quả mong đợi:

- có worker kết nối vào master
- có app `RealtimeFraud3StreamIntegration`
- có các streaming query đang chạy

## 20. Step 15: Kiểm tra Streamlit

Mục tiêu:

- xác nhận lớp quan sát đầu ra đang lấy được dữ liệu từ Redis/Cassandra

Mở:

- `http://localhost:8501`

Kết quả mong đợi:

- thấy summary alert
- thấy metrics theo window
- thấy dữ liệu hiển thị ổn định

## 21. Step 16: Kiểm tra Cassandra

Mục tiêu:

- xác nhận dữ liệu tích hợp hợp lệ đã được ghi xuống sink chính

Lệnh:

```powershell
docker exec -it cassandra cqlsh
```

Trong `cqlsh`, chạy:

```sql
USE fraud_detection;
SELECT * FROM transactions_by_day LIMIT 10;
SELECT * FROM alerts_by_account LIMIT 10;
SELECT * FROM metrics_by_window LIMIT 10;
SELECT * FROM account_state_by_account LIMIT 10;
SELECT * FROM processed_stream_batches LIMIT 20;
```

Kết quả mong đợi:

- có transaction đã tích hợp thành công
- có metrics theo window
- có alert nếu dữ liệu đủ điều kiện fraud

## 22. Step 17: Kiểm thử tích hợp streaming end-to-end

Mục tiêu:

- kiểm tra luồng tích hợp thật trên Kafka + Spark + Cassandra
- chứng minh hệ thống không làm mất im lặng record lỗi

Lệnh:

```powershell
python .\scripts\validate_streaming_integration.py
python .\scripts\validate_streaming_integration.py --json-out .\streaming-validation.json
```

Script sẽ tự:

- lấy 1 bộ mẫu từ `Data/logical_sources`
- tạo các ca kiểm thử có chủ đích
- publish lên `transaction_topic`, `sender_state_topic`, `receiver_state_topic`
- chờ Spark xử lí
- kiểm tra `pipeline_dead_letter`
- kiểm tra transaction hợp lệ đã được ghi vào Cassandra

Các ca kiểm thử hiện có:

- `clean_integration`
- `missing_sender_state`
- `missing_receiver_state`
- `semantic_mismatch`
- `orphan_sender_state`
- `orphan_receiver_state`

Kết quả mong đợi:

- `clean_integration` phải được ghi vào Cassandra
- các ca lỗi phải xuất hiện trong `pipeline_dead_letter` với đúng loại lỗi
- script trả exit code `0` nếu tất cả case đều đạt

Nếu script fail:

- xem lại log `spark-app`
- mở Kafka UI để kiểm tra `pipeline_dead_letter`
- kiểm tra Cassandra có nhận transaction hợp lệ hay không

## 23. Step 18: Tăng tải dần để benchmark

Mục tiêu:

- đo ảnh hưởng của tải lên latency và throughput
- chuẩn bị số liệu cho phần đánh giá hệ thống

Ví dụ:

```powershell
python .\scripts\bootstrap_local_stack.py --max-events 100 --rate 25
python .\scripts\bootstrap_local_stack.py --max-events 500 --rate 50
python .\scripts\bootstrap_local_stack.py --max-events 1000 --rate 100
```

Khuyến nghị:

- không nhảy ngay lên tải rất lớn
- sau mỗi lần tăng tải, kiểm tra:
  `spark-app` log
  `metrics_windowed`
  Kafka UI
  Spark UI
  Cassandra

Cho phần báo cáo, bạn nên ghi lại:

- `max-events`
- `rate`
- thời gian xử lí
- số alert
- số dead-letter
- độ trễ quan sát được

## 24. Quy trình ngắn gọn nếu bạn muốn chạy từ đầu

```powershell
cd F:\Project\Bigdata
python -m pip install -r requirements-local.txt
python -m unittest discover -s tests -v
python .\scripts\smoke_local_pipeline.py --limit 20
python .\scripts\benchmark_local_pipeline.py --seed-limit 20 --profiles smoke=100
docker compose down -v
docker compose up -d
python .\scripts\check_remote_services.py --json-out .\service-report.json
python .\scripts\split_logical_sources.py --max-events 100
python .\scripts\publish_risk_rules.py
python .\scripts\publish_logical_sources_parallel.py --source-dir .\Data\logical_sources --max-events 100 --rate 20
python .\scripts\validate_streaming_integration.py --json-out .\streaming-validation.json
```

## 25. Khi nào coi là hệ thống chạy đúng

Bạn có thể coi local stack đang chạy đúng khi đồng thời thỏa các điều kiện sau:

- unit test pass
- smoke test chạy ổn
- Docker service healthy
- 3 topic nguồn nhận được message
- Spark app chạy và có query streaming
- Spark UI ở `4040` mở được
- Cassandra có transaction và metrics
- validation end-to-end pass
- record lỗi đi vào `pipeline_dead_letter`, không bị mất im lặng
- [service-report.json](/f:/Project/Bigdata/service-report.json) báo tất cả service `ok`

## 26. File liên quan

- [README.md](/f:/Project/Bigdata/README.md)
- [docker-compose.yml](/f:/Project/Bigdata/docker-compose.yml)
- [split_logical_sources.py](/f:/Project/Bigdata/scripts/split_logical_sources.py)
- [publish_risk_rules.py](/f:/Project/Bigdata/scripts/publish_risk_rules.py)
- [publish_logical_sources_parallel.py](/f:/Project/Bigdata/scripts/publish_logical_sources_parallel.py)
- [bootstrap_local_stack.py](/f:/Project/Bigdata/scripts/bootstrap_local_stack.py)
- [validate_streaming_integration.py](/f:/Project/Bigdata/scripts/validate_streaming_integration.py)
- [stream_job.py](/f:/Project/Bigdata/spark-app/stream_job.py)
- [REALTIME_FRAUD_PIPELINE_DESIGN.md](/f:/Project/Bigdata/docs/REALTIME_FRAUD_PIPELINE_DESIGN.md)
