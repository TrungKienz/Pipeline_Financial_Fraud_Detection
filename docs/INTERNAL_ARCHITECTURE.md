# 🏗️ TÀI LIỆU KIẾN TRÚC VÀ THÔNG SỐ KỸ THUẬT (SENIOR LEVEL)

Tài liệu này mô tả chi tiết cơ chế vận hành nội bộ của hệ thống Real-time Fraud Detection.

## 1. LUỒNG TÍCH HỢP DỮ LIỆU (DATA INTEGRATION FLOW)
Hệ thống giải quyết bài toán tích hợp từ 3 luồng Kafka độc lập thông qua Spark Structured Streaming:

1.  **Transaction Stream:** Đóng vai trò là luồng điều hướng (Driving stream).
2.  **State Streams (Sender/Receiver):** Đóng vai trò là luồng làm giàu (Enrichment streams).

### Cơ chế Join:
-   Sử dụng **Stream-Stream Join** với ràng buộc thời gian (Interval Join).
-   **Join Condition:** `tx.event_id = state.source_event_id` VÀ `state.event_time` nằm trong khoảng +/- 30 giây so với `tx.event_time`.
-   **Watermarking:** 10 phút cho cả 3 luồng để quản lý bộ nhớ state và xử lý dữ liệu đến muộn.

## 2. BỘ MÁY PHÁT HIỆN GIAN LẬN (FRAUD ENGINE)
Kiến trúc **Hybrid** kết hợp:

### A. Rule-based (Trọng số 60%):
-   `high_amount_transfer`: Ngưỡng số tiền (Configurable qua Kafka).
-   `balance_inconsistency`: Kiểm tra logic số dư trước và sau giao dịch.
-   `rapid_outflow_pattern`: Kiểm tra tần suất giao dịch của người gửi trong 1 giờ qua (Sử dụng Redis Sorted Sets).

### B. Machine Learning (Trọng số 40%):
-   Mô hình Random Forest (Simulated) chấm điểm xác suất gian lận dựa trên hành vi giao dịch.
-   Kết quả cuối cùng: `Combined_Score = (Rule_Score * 0.6) + (ML_Score * 0.4)`.

## 3. THÔNG SỐ HẠ TẦNG (INFRASTRUCTURE SPECS)

| Thành phần | Công nghệ | Cấu hình nổi bật |
| :--- | :--- | :--- |
| **Message Bus** | Kafka KRaft | 7 Topics, Replication Factor 1 (Local), Partitioning 6. |
| **Stream Proc** | Spark 3.5.1 | Checkpoint-based fault tolerance, Exact-once processing. |
| **Durable Store** | Cassandra 4.1 | Schema-per-day partitioning cho transactions. |
| **Hot Store** | Redis 7 | TTL 24h cho các alerts nóng, sliding window history. |
| **Monitoring** | Prometheus | Scrape interval 5s, Spark JMX integration. |

## 4. XỬ LÝ LỖI VÀ TOÀN VẸN DỮ LIỆU
-   **Dead Letter Queue (DLQ):** Mọi bản ghi không thể join hoặc sai schema đều được đẩy vào topic `pipeline_dead_letter` kèm theo `error_reason`.
-   **Idempotency:** Sử dụng `processed_stream_batches` trong Cassandra để đảm bảo không ghi trùng dữ liệu khi Spark job restart.

---
*Mọi thay đổi về kiến trúc phải được cập nhật vào tài liệu này và thông báo cho Team Lead.*
