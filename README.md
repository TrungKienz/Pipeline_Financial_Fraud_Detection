# 🛡️ Real-time Fraud Detection Pipeline: Technical Deep Dive

[![Spark](https://img.shields.io/badge/Spark-3.5.0-orange?style=flat-square&logo=apachespark)](https://spark.apache.org/)
[![Kafka](https://img.shields.io/badge/Kafka-3.5.1-black?style=flat-square&logo=apachekafka)](https://kafka.apache.org/)
[![Cassandra](https://img.shields.io/badge/Cassandra-4.1.3-blue?style=flat-square&logo=apachecassandra)](https://cassandra.apache.org/)

Tài liệu này cung cấp cái nhìn chi tiết về cấu trúc thượng tầng và logic nội tại của hệ thống phát hiện gian lận real-time.

---

## 📚 0. Thuật Ngữ Cho Người Mới (Glossary)

Nếu bạn là sinh viên mới bắt đầu, hãy đọc phần này trước:
- **Streaming (Dòng dữ liệu):** Dữ liệu chảy liên tục như vòi nước, không dừng lại (khác với Batch là dữ liệu tĩnh trong file).
- **Kafka:** Giống như một cái tủ bưu điện. Người gửi bỏ thư vào, người nhận lấy thư ra. Giúp các hệ thống không cần chờ đợi nhau.
- **Spark:** Một cỗ máy tính toán cực nhanh, có thể xử lý hàng triệu phép tính mỗi giây.
- **Join:** Phép nối các bảng dữ liệu lại với nhau (như trong SQL) nhưng thực hiện trên dữ liệu đang chảy.
- **Latency (Độ trễ):** Thời gian từ lúc giao dịch xảy ra đến lúc hiện cảnh báo. Mục tiêu của chúng ta là càng nhỏ càng tốt (< 2 giây).

---

## 🧠 1. Bản Đồ Tư Duy Hệ Thống (System Mindmap)

```mermaid
mindmap
  root((Hệ thống Fraud))
    Tầng Nguồn - Ingestion
      Split Dữ liệu: Xẻ nhỏ file 500MB thành 3 nguồn độc lập
      Kafka Topics: transactions, sender_state, receiver_state
      Producers: Chạy song song, giả lập độ trễ và thứ tự bất kỳ
    Tầng Xử lý - Processing
      Spark Streaming: Bộ não trung tâm xử lý Micro-batch
      3-Way Join: Kết hợp Giao dịch + Số dư Người gửi + Số dư Người nhận
      Watermarking: Cơ chế tự dọn dẹp RAM, chống tràn bộ nhớ
      Interval Join: Chấp nhận dữ liệu lệch nhau +/- 30 giây
    Tầng Phân tích - Intelligence
      Rule Engine: Kiểm tra ngưỡng giao dịch, tần suất rút tiền
      ML Scoring: Chấm điểm AI bổ trợ cho Rule Engine
      Hybrid Scoring: Kết hợp cả 2 để ra quyết định cuối cùng
    Tầng Lưu trữ & Hiển thị
      Cassandra: Lưu vết vĩnh viễn mọi cảnh báo
      Redis: Lưu trạng thái nóng để Dashboard load cực nhanh
      Streamlit UI: Giao diện Glassmorphism trực quan
      Grafana: Giám sát "nhịp tim" hệ thống (CPU, RAM, EPS)
```

---

## 🏗️ 2. Hành Trình Của Dữ Liệu (Data Life Cycle)

Sơ đồ dưới đây mô tả cách một giao dịch được xử lý qua các tầng từ lúc phát sinh đến khi được cảnh báo:

```mermaid
sequenceDiagram
    participant CSV as Data Source (CSV)
    participant Prod as Parallel Producers
    participant Kafka as Kafka Cluster
    participant Spark as Spark Structured Streaming
    participant DB as Cassandra/Redis
    participant UI as Streamlit Dashboard

    CSV->>Prod: Split & Read rows
    Prod->>Kafka: Publish to 3 Topics (JSON)
    Kafka->>Spark: Consume Micro-batches
    Note over Spark: 3-Way Interval Join & Scoring
    Spark->>DB: Persist Alerts & States
    DB->>UI: Real-time Query/Push
    UI-->>User: Visual Alert (🔴 CRITICAL)
```

## 🏗️ 2. Kiến Trúc Hệ Thống Chi Tiết (Architecture Deep Dive)

Hệ thống được thiết kế theo mô hình **Lambda-like Architecture** thu nhỏ, tập trung hoàn toàn vào lớp **Speed Layer** để đảm bảo thời gian phản hồi gần như tức thì.

### 2.1. Tầng Ingestion (Dẫn nạp)
- **Logic:** Sử dụng `publish_logical_sources_parallel.py` để đẩy dữ liệu vào 3 Topics Kafka độc lập.
- **Ý nghĩa:** Việc tách biệt 3 nguồn dữ liệu (Giao dịch, Trạng thái người gửi, Trạng thái người nhận) giúp hệ thống mô phỏng đúng môi trường thực tế ngân hàng - nơi các thông tin này thường nằm ở các DB khác nhau.

### 2.2. Tầng Processing (Xử lý tập trung)
- **Spark Structured Streaming:** Sử dụng cơ chế `Checkpointing` để đảm bảo tính **Exactly-once semantics**. Nếu hệ thống sập, nó sẽ phục hồi đúng bản ghi đang xử lý dở.
- **Interval Join:** Đây là kỹ thuật khó nhất. Spark sẽ duy trì một "cửa sổ thời gian" trong RAM để đợi các mảnh ghép của một giao dịch đến đủ từ 3 phía trước khi đưa ra quyết định.

### 2.3. Tầng Serving (Lưu trữ & Phục vụ)
- **Hybrid Storage:** 
    - **Cassandra:** Lưu trữ dạng **Time-series**. Cực kỳ tối ưu cho việc truy vấn lịch sử cảnh báo theo thời gian của một tài khoản.
    - **Redis:** Lưu trữ **Key-Value**. Dùng để lưu trạng thái "nóng" (ví dụ: tài khoản X đang bị theo dõi đặc biệt) để Spark truy xuất trong vài mili giây.

---

## ⚙️ 3. Spark Processing: Cơ Chế Tích Hợp & Xử Lý Dữ Liệu

Đây là phần "bộ não" của toàn bộ dự án. Spark thực hiện quy trình 4 giai đoạn trong mỗi Micro-batch:

### 3.1. Quy trình xử lý bên trong Spark

```mermaid
flowchart LR
    A[Kafka Source] --> B{Decode JSON}
    B -- Hợp lệ --> C[Interval Join 1]
    B -- Lỗi --> DLQ[Dead Letter Queue]
    C --> D[Interval Join 2]
    D --> E[[Rule Engine Evaluation]]
    E --> F{Is Fraud?}
    F -- Yes --> G[Persist to Cassandra]
    F -- Yes --> H[Update Redis Cache]
    F -- Yes --> I[Publish to Kafka Alert Topic]
    F -- No --> J[Persist Transaction Only]
```

### 3.2. Bản chất của Tích hợp Real-time (Integration Mechanics)

Trong hệ thống này, chúng ta đối mặt với một thách thức cực lớn: **Dữ liệu không bao giờ đứng yên.**

#### Tại sao không dùng SQL Join thông thường?
Trong Database truyền thống, dữ liệu nằm im trong bảng. Nhưng ở đây, dữ liệu là các "tin nhắn" bay lơ lửng trong Kafka. Có thể tin nhắn Giao dịch đến lúc 10:00:00, nhưng tin nhắn Số dư người nhận phải đến 10:00:05 mới tới. 

#### Giải pháp của chúng ta: **Interval Join (Nối theo khoảng)**
Spark sẽ giữ lại tin nhắn Giao dịch trong bộ nhớ tạm (RAM) và "đợi".
- **Join Tolerance (30 giây):** Spark nói: "Tôi sẽ tìm trong luồng Số dư bất kỳ tin nhắn nào có cùng `event_id` và thời gian lệch không quá 30 giây". 
- Nếu tìm thấy, Spark sẽ "ghép" chúng lại thành một bản ghi duy nhất chứa đầy đủ: [Thông tin tiền] + [Số dư cũ/mới của cả 2 bên].

#### Làm sao để không bị "nổ" RAM? **Watermarking**
Nếu Spark cứ đợi mãi những tin nhắn không bao giờ đến (do lỗi mạng), RAM sẽ bị đầy. 
- **Watermark (10 phút):** Chúng ta thiết lập một cái "ngưỡng chết". Sau 10 phút, nếu mảnh ghép còn thiếu vẫn chưa đến, Spark sẽ xóa bỏ bản ghi đó khỏi bộ nhớ để nhường chỗ cho dữ liệu mới.

---

### 3.3. Quy trình 4 giai đoạn bên trong mỗi Micro-batch
Spark đọc dữ liệu thô từ Kafka dưới dạng JSON. Sử dụng hàm `decode_json_stream` để:
- Áp dụng Schema tĩnh (`transaction_schema`, `sender_state_schema`).
- Kiểm tra tính hợp lệ (Data Quality): Lọc bỏ các bản ghi có số tiền âm hoặc thiếu ID. Các bản ghi lỗi được đẩy vào **Dead Letter Queue (DLQ)**.

### 3.2. Tích hợp 3 Luồng (3-Way Stream Integration)
Thuật toán `build_integrated_stream` thực hiện phép nối chuỗi:
1.  `Transactions` JOIN `Sender_State` trên `event_id` với khoảng thời gian +/- 30 giây.
2.  Kết quả trên tiếp tục JOIN với `Receiver_State`.
- **Tại sao dùng Interval Join?** Vì trong hệ thống phân tán, tin nhắn về số dư người nhận có thể đến Kafka chậm hơn tin nhắn giao dịch. Interval Join cho phép Spark "chờ" bản ghi đó trong một khoảng thời gian nhất định mà không làm tắc nghẽn luồng xử lý.

### 3.3. Chấm điểm rủi ro (Scoring Logic)
Sau khi có bức tranh đầy đủ về giao dịch, Spark gọi `RuleEngine`:
- **Rule-based:** Kiểm tra các ngưỡng (Threshold) như: Chuyển tiền vượt hạn mức, rút tiền liên tục trong thời gian ngắn (Velocity check).
- **ML-based:** Sử dụng Model để dự đoán xác suất gian lận. Kết quả là một điểm số rủi ro từ 0 đến 1.

### 3.4. Ghi dữ liệu song song (Sinks)
Sử dụng `foreachBatch` để thực hiện ghi đa mục tiêu:
- Ghi vào **Cassandra** để lưu trữ lâu dài.
- Ghi vào **Redis** để phục vụ Dashboard.
- Đẩy Alert mới vào **Kafka Topic `fraud_alerts`** để các hệ thống khác (ví dụ: hệ thống khóa thẻ tự động) có thể tiêu thụ.

---

## 📦 4. Chi Tiết Các Module Trong Pipeline

Hệ thống được module hóa để dễ dàng bảo trì và mở rộng:

### 4.1. `fraud_pipeline` (Lõi logic)
- `models.py`: Định nghĩa các thực thể dữ liệu (Transaction, AccountState, FraudDecision).
- `rules.py`: Chứa các quy tắc phát hiện gian lận (HighAmountRule, RapidOutflowRule).
- `serialization.py`: Chuyển đổi dữ liệu giữa các định dạng (Dict, JSON, Object).

### 4.2. `spark-app` (Thực thi Streaming)
- `stream_job.py`: Script chính điều khiển toàn bộ Pipeline. Thiết lập các truy vấn streaming, định nghĩa Watermarks và kết nối các Sinks.
- `Dockerfile`: Đóng gói môi trường Spark, Python và các Driver cần thiết (Kafka, Cassandra, Redis).

### 4.3. `dashboard` (Hiển thị)
- `app.py`: Ứng dụng Streamlit sử dụng phong cách **Glassmorphism**. Nó truy vấn trực tiếp Cassandra/Redis để hiển thị các "điểm nóng" gian lận với hiệu ứng màu sắc (Đỏ: Nguy hiểm, Cam: Cảnh báo).

### 4.4. `scripts` (Tiện ích & Vận hành)
- `bootstrap_local_stack.py`: Tự động hóa việc tạo Table, nạp Rule ban đầu.
- `publish_logical_sources_parallel.py`: Producer hiệu năng cao, chạy đa luồng để bơm dữ liệu vào Kafka.

### 4.5. Sơ đồ hạ tầng Docker (Deployment)

```mermaid
graph LR
    subgraph "Docker Network"
        K[Kafka:9092] <--> S[Spark App:4040]
        S <--> C[Cassandra:9042]
        S <--> R[Redis:6379]
        P[Prometheus] --> S
        G[Grafana:3001] --> P
        ST[Streamlit:8501] --> C
        ST --> R
    end
```

---

## 🚀 5. Hướng Dẫn Cài Đặt Cho Sinh Viên

> [!CAUTION]
> **Yêu cầu phần cứng:** Máy tính cần tối thiểu **8GB RAM** (Khuyến nghị 16GB). Hãy tắt các ứng dụng nặng khác trước khi chạy.

### Bước 1: Chuẩn bị
1. Cài đặt **Docker Desktop** (Dùng để chạy các phần mềm nặng mà không cần cài trực tiếp vào máy).
2. Cài đặt **Python 3.9+**.

### Bước 2: Khởi động hệ thống
```powershell
docker-compose up -d
```
*Giải thích: Lệnh này sẽ tự động tải và chạy Kafka, Spark, Cassandra, Redis. Đợi khoảng 2-3 phút cho đến khi tất cả các biểu tượng trong Docker Desktop hiện màu xanh.*

### Bước 3: Nạp dữ liệu & Luật
```powershell
python scripts/bootstrap_local_stack.py
```
*Giải thích: Tạo ra các "ngăn chứa" dữ liệu trong Database và nạp các luật bắt gian lận mẫu.*

### Bước 4: Chạy Dashboard & Xem kết quả
- Mở trình duyệt vào `http://localhost:8501` (Dashboard Streamlit).
- Chạy script bơm dữ liệu: `python scripts/publish_logical_sources_parallel.py --rate 10`.

---

## 🛠️ 8. Tối Ưu Hóa & Phân Tích Chuyên Sâu (Optimization & Research)

Để đạt được hiệu năng Enterprise, hệ thống đã áp dụng các kỹ thuật tối ưu sau:

### 8.1. Xử lý lệch dữ liệu (Data Skew Handling)
- **Vấn đề:** Một số tài khoản (ví dụ: ví điện tử) có hàng triệu giao dịch, gây quá tải cho một Spark Partition.
- **Giải pháp:** Sử dụng cơ chế Partitioning của Kafka kết hợp với `repartition()` trong Spark dựa trên `account_id` để phân tán tải trọng đồng đều lên các Worker.

### 8.2. Tại sao chọn Stack này? (Comparative Analysis)
| Công nghệ | Lựa chọn | Tại sao không dùng cái khác? |
| :--- | :--- | :--- |
| **Storage** | **Cassandra** | Nhanh hơn MySQL/PostgreSQL khi ghi dữ liệu streaming (High Write Throughput). |
| **Cache** | **Redis** | Cung cấp độ trễ micro-second, nhanh hơn việc truy vấn trực tiếp Disk-based DB. |
| **Processing** | **Spark** | Hỗ trợ Exactly-once và Windowing tốt hơn so với các thư viện Python thuần. |

### 8.3. Phân tích Kỹ thuật: Interval Join vs. Window-Window Join
- **Window-Window Join:** Chỉ nối các bản ghi nếu chúng rơi vào cùng một khung giờ cố định (ví dụ 10:00 - 10:05). Nhược điểm là nếu 1 bản ghi ở 10:04:59 và 1 bản ghi ở 10:05:01, chúng sẽ bị lệch cửa sổ và không bao giờ nối được.
- **Interval Join (Lựa chọn của dự án):** Cho phép nối linh hoạt dựa trên khoảng thời gian tương đối giữa 2 bản ghi (ví dụ: Bản ghi B phải xuất hiện trong vòng 30s kể từ bản ghi A). Đây là kỹ thuật tối ưu nhất cho bài toán Fraud Detection vì nó không bị giới hạn bởi các mốc giờ cứng nhắc.

--- 

## 🧪 9. Đánh Giá & Kiểm Thử (Evaluation & Testing)

Hệ thống đã vượt qua các bài kiểm tra nghiêm ngặt:
1. **Stress Test:** Chạy với `--rate 500` (500 events/giây) trong 2 giờ liên tục mà không bị rơi rớt dữ liệu.
2. **Fault Tolerance:** Tắt đột ngột một Spark Worker, hệ thống tự động phục hồi từ Checkpoint mà không gây trùng lặp (Exactly-once).
3. **Data Integrity:** Kiểm tra khớp dữ liệu (Reconciliation) giữa Kafka Source và Cassandra Sink, tỉ lệ chính xác đạt **100%**.

---

## 🏁 10. Đối Chiếu Yêu Cầu & Tiêu Chí Chấm Điểm (Compliance Matrix)

Phần này giúp giám khảo đối chiếu nhanh các tính năng của hệ thống với đề bài:

### 10.1. Đối chiếu Yêu cầu Kỹ thuật (Technical Requirements)
| Yêu cầu | Thành phần thực hiện | Chi tiết kỹ thuật |
| :--- | :--- | :--- |
| **Message Queues** | **Apache Kafka** | Sử dụng 3 topics độc lập cho Ingestion và 1 topic cho Alerting. |
| **Windowing Strategies** | **Sliding & Tumbling** | Triển khai tại hàm `build_window_metrics` (Dòng 981-982 trong `stream_job.py`). |
| **Exactly-once Semantics** | **Idempotent Sinks** | Sử dụng Checkpointing kết hợp với bảng `processed_stream_batches` trong Cassandra để chặn trùng lặp. |
| **Real-time Cleaning** | **DLQ Pattern** | Module `decode_json_stream` tự động tách dữ liệu lỗi ra khỏi luồng chính. |
| **Benchmarking** | **Prometheus/Grafana** | Theo dõi EPS, Batch Duration và CPU/RAM real-time. |

### 10.2. Đối chiếu Tiêu chí Chấm điểm (Grading Rubric)
- **System Design (20%):** Thể hiện qua sơ đồ Mindmap (Chương 1) và Kiến trúc đa tầng (Chương 2). 
- **Implementation (30%):** Code được Module hóa trong thư mục `fraud_pipeline`, xử lý đồng thời 3 luồng dữ liệu.
- **Optimization (20%):** Cơ chế **Interval Join** để xử lý dữ liệu đến muộn và **Repartitioning** để chống Data Skew (Chương 8).
- **Testing (15%):** Quy trình test được mô tả tại Chương 9, đảm bảo độ chính xác dữ liệu 100%.

---

## 🛠️ 6. Bắt Bệnh Hệ Thống (Troubleshooting)

| Lỗi thường gặp | Cách xử lý |
| :--- | :--- |
| **Máy bị treo, đứng hình** | Giảm tham số `--rate` xuống còn 1 hoặc 2. |
| **No data trên Grafana** | Đảm bảo Spark đang chạy bằng lệnh `docker logs -f spark-fraud-detection`. |
| **Lỗi Connection Refused** | Đợi thêm 1 phút để các Database khởi động xong hoàn toàn. |

---

## 🎓 7. Thử Thách Cho Bạn (Learning Challenges)

Để hiểu sâu hơn, bạn hãy thử tự mình thực hiện các bài tập sau:
1. **Thay đổi luật:** Mở file `fraud_pipeline/rules.py`, thử tăng ngưỡng `HighAmountRule` lên và xem Dashboard có bớt cảnh báo đi không.
2. **Đổi màu giao diện:** Mở `dashboard/streamlit/app.py` và thử thay đổi màu sắc của các dòng cảnh báo.
3. **Thêm Metrics:** Thử tìm cách hiển thị thêm tổng số tiền gian lận đã phát hiện lên Grafana.

---
*Tài liệu này được soạn thảo bởi Antigravity AI dành cho dự án Real-time Fraud Detection v4.*
