# 🛠️ NHẬT KÝ XỬ LÝ LỖI VÀ FAQ (TROUBLESHOOTING)

Tài liệu tổng hợp các lỗi thường gặp trong quá trình vận hành hệ thống Big Data này.

### 1. Lỗi: "Cassandra connection refused"
-   **Nguyên nhân:** Container Cassandra chưa khởi động xong hoặc bị Crash do thiếu RAM.
-   **Cách kiểm tra:** `docker logs cassandra`. Nếu thấy lỗi `OOM Score`, hãy tăng RAM cho Docker Desktop lên 8GB.
-   **Xử lý:** Chạy `python scripts/bootstrap_local_stack.py` một lần nữa sau khi đã tăng RAM.

### 2. Lỗi: "Kafka topic not found"
-   **Nguyên nhân:** Bước khởi tạo Topic bị lỗi hoặc Kafka chưa sẵn sàng khi script chạy.
-   **Cách kiểm tra:** Truy cập `http://localhost:8085` xem danh sách topic.
-   **Xử lý:** Chạy lại script khởi tạo hoặc tạo thủ công bằng lệnh Kafka CLI trong container.

### 3. Lỗi: Spark Job không xử lý dữ liệu (Batch size = 0)
-   **Nguyên nhân:** Lệch thời gian giữa 3 nguồn dữ liệu lớn hơn `JOIN_TOLERANCE` (30 giây).
-   **Cách kiểm tra:** Xem tin nhắn trong topic `pipeline_dead_letter`. Nếu thấy lỗi `missing_sender_state`, nghĩa là luồng Sender đến quá muộn.
-   **Xử lý:** Kiểm tra lại script đẩy dữ liệu hoặc tăng tham số `JOIN_TOLERANCE` trong `stream_job.py`.

### 4. Lỗi: Dashboard Streamlit không hiện Alert
-   **Nguyên nhân:** Redis hoặc Cassandra chưa có dữ liệu, hoặc Rule quá khắt khe.
-   **Cách kiểm tra:** Kiểm tra bảng `alerts_by_account` trong Cassandra.
-   **Xử lý:** Đẩy thêm dữ liệu bằng `stress_test_pipeline.py` và giảm ngưỡng rủi ro trong `rules.py` để test.

### 5. Câu hỏi thường gặp (FAQ)
-   **Q: Làm sao để xóa sạch toàn bộ dữ liệu để làm lại từ đầu?**
    -   A: Chạy `docker-compose down -v`. Lệnh này sẽ xóa sạch các Volumes dữ liệu.
-   **Q: Tôi có thể thay đổi luật phát hiện gian lận mà không dừng Spark không?**
    -   A: Có, nếu bạn bật biến môi trường `RISK_RULE_REFRESH_SECONDS > 0`. Spark sẽ tự động đọc lại các luật mới từ Kafka topic `risk_rules`.

---
*Nếu gặp lỗi lạ chưa có trong danh sách, hãy chụp màn hình log và gửi cho Senior phụ trách.*
