# Data Dictionary — Fraud Detection Feature Table

Mô tả mọi trường trong bảng feature dùng để train/serve model (`model/final_feature_table.csv`,
sinh bởi `model/train_model.py:export_final_feature_table`). Feature được định nghĩa trong
`fraud_pipeline/features.py`; các feature có trạng thái (stateful) được tính trong
`model/train_model.py:StateSimulator`.

## Phân loại nguồn (`source`)

- **raw** — lấy trực tiếp từ dataset PaySim gốc (`fraud_pipeline/parsing.py`).
- **derived-real** — suy ra **hoàn toàn từ dữ liệu giao dịch thật** (số dư, số tiền, thời gian, lịch sử). Đây là nhóm tín hiệu fraud chính.
- **synthetic** — trường mô phỏng "contextual e-commerce" mà PaySim không có. Được sinh **độc lập với nhãn** `is_fraud`, dùng hàm băm `md5(event_id)` (tái lập được) và xác suất tỉ lệ theo `_risk_proxy(event)` — một điểm rủi ro tính từ thuộc tính thật (`type`, `amount`, độ vét số dư, giao dịch ban đêm). Vì không đọc nhãn nên **không gây target leakage và nhất quán giữa train ↔ serving**.
- **id / label** — không dùng làm feature đầu vào.

> ⚠️ Lưu ý về nhóm `synthetic`: các trường này là **dữ liệu mô phỏng**, không phải hành vi người dùng thật. Tương quan với fraud là gián tiếp (qua `_risk_proxy`), **không phải rò rỉ nhãn**. Chúng chủ yếu để minh hoạ khung feature e-commerce theo đề bài; tín hiệu dự báo thực nằm ở nhóm `derived-real`.

## Bảng trường

| # | Field | Dtype | Source | Definition | Ghi chú tín hiệu fraud |
|---|-------|-------|--------|------------|------------------------|
| — | `event_id` | str | id | Khoá sự kiện duy nhất | Không phải feature |
| — | `label_is_fraud` | int {0,1} | label | Nhãn gian lận (PaySim `isFraud`) | Mục tiêu dự báo, **không** đưa vào X |
| 1 | `step` | int | raw | Bước thời gian PaySim (giờ kể từ mốc) | Yếu; dùng để suy `hour_of_day` |
| 2 | `amount` | float | raw | Số tiền giao dịch | Fraud thường số tiền lớn |
| — | `txn_type` | str | raw | Loại giao dịch (được one-hot thành `type_*`) | Fraud tập trung ở TRANSFER/CASH_OUT |
| 3 | `sender_balance_delta` | float | derived-real | `oldbalance_org - newbalance_orig` | Lượng tiền rời tài khoản gửi |
| 4 | `receiver_balance_delta` | float | derived-real | `newbalance_dest - oldbalance_dest` | Lượng tiền vào tài khoản nhận |
| 5 | `sender_depletion_ratio` | float [0,1] | derived-real | `min(amount / oldbalance_org, 1)` | Vét sạch số dư → tín hiệu mạnh |
| 6 | `amount_to_balance_ratio` | float [0,1] | derived-real | `amount / (oldbalance_org + oldbalance_dest)` | Tỉ trọng số tiền so với số dư |
| 7 | `is_zero_balance_after` | int {0,1} | derived-real | Số dư gửi = 0 sau giao dịch (loại debit) | Rút cạn tài khoản |
| 8 | `is_same_sender_receiver` | int {0,1} | derived-real | `name_orig == name_dest` | Bất thường |
| 9 | `sender_balance_inconsistent` | int {0,1} | derived-real | Sai lệch số dư gửi vượt `balance_tolerance` | Số dư không khớp toán học |
| 10 | `receiver_balance_inconsistent` | int {0,1} | derived-real | Sai lệch số dư nhận vượt ngưỡng | Số dư không khớp |
| 11 | `dest_is_merchant` | int {0,1} | derived-real | `name_dest` bắt đầu bằng "M" | Merchant vs tài khoản cá nhân |
| 12 | `hour_of_day` | int 0–23 | derived-real | `step % 24` | Chu kỳ theo giờ |
| 13 | `sender_balance_discrepancy` | float | derived-real | `(oldbalance_org - amount) - newbalance_orig` | Độ lệch số dư gửi |
| 14 | `receiver_balance_discrepancy` | float | derived-real | `(oldbalance_dest + amount) - newbalance_dest` | Độ lệch số dư nhận |
| 15 | `is_night_transaction` | int {0,1} | derived-real | Giờ ∈ [22, 6] | Giao dịch ban đêm |
| 16 | `new_device_flag` | int {0,1} | **synthetic** | `hash(event_id) < 0.05 + 0.35·risk_proxy` | Mô phỏng thiết bị lạ; **label-free** |
| 17 | `shipping_billing_mismatch` | int {0,1} | **synthetic** | `hash(event_id) < 0.02 + 0.28·risk_proxy` | Mô phỏng lệch địa chỉ; **label-free** |
| 18 | `ip_billing_country_mismatch` | int {0,1} | **synthetic** | `hash(event_id) < 0.03 + 0.22·risk_proxy` | Mô phỏng lệch quốc gia IP; **label-free** |
| 19 | `failed_payment_attempts_24h` | int 0–3 | **synthetic** | Số lần fail ~ tăng theo `risk_proxy` | Mô phỏng thử thanh toán hỏng; **label-free** |
| 20 | `sender_recent_txn_count` | float | derived-real | Số giao dịch gửi trong cửa sổ fan-out | Velocity chi tiền (fan-out) |
| 21 | `sender_recent_total_amount` | float | derived-real | Tổng tiền gửi trong cửa sổ | Fan-out amount |
| 22 | `receiver_recent_txn_count` | float | derived-real | Số giao dịch vào tài khoản nhận (fan-in) | Fan-in |
| 23 | `receiver_recent_total_amount` | float | derived-real | Tổng tiền vào trong cửa sổ | Fan-in amount |
| 24 | `is_new_counterparty` | int {0,1} | derived-real | Chuyển tới đối tác chưa từng gặp | Đối tác mới |
| 25 | `inbound_to_cashout_ratio` | float | derived-real | Tỉ lệ tiền vừa nhận rồi rút ra | Cash-out sau khi nhận |
| 26 | `velocity_transactions_1h` | float | derived-real | Số giao dịch của người gửi trong 1h | Tần suất giao dịch |
| 27 | `time_since_last_purchase` | float (giây) | derived-real | Khoảng cách tới giao dịch trước (mặc định 86400) | Hành vi theo thời gian |
| 28–32 | `type_{CASH_IN,CASH_OUT,DEBIT,PAYMENT,TRANSFER}` | int {0,1} | encoded (raw) | One-hot của `txn_type` | TRANSFER/CASH_OUT rủi ro cao |
| 33–36 | `browser_{chrome,safari,firefox,edge}` | int {0,1} | **synthetic** | One-hot của `browser` (gần uniform, `hash(event_id)`) | Tín hiệu yếu; minh hoạ |
| 37–39 | `device_type_{desktop,mobile,tablet}` | int {0,1} | **synthetic** | One-hot của `device_type` (risk cao → nghiêng mobile/tablet) | **label-free** |
| 40–44 | `country_{US,VN,SG,PH,TH}` | int {0,1} | **synthetic** | One-hot của `country` (phân phối lệch theo `risk_proxy`) | **label-free** |

## `_risk_proxy` (không đọc nhãn)

Định nghĩa tại `fraud_pipeline/features.py:_risk_proxy`:

```
risk_proxy = 0.35·(type ∈ {TRANSFER, CASH_OUT})
           + 0.25·min(amount / 200000, 1)
           + 0.25·sender_depletion_ratio
           + 0.15·is_night_transaction     # ∈ [0, 1]
```

Các trường `synthetic` lấy mẫu bằng `_hash_unit(event_id, salt)` (uniform tái lập từ `md5`) so với xác suất phụ thuộc `risk_proxy`. Vì không hàm nào tham chiếu `event.is_fraud`, feature vector giống hệt nhau khi đổi nhãn — được kiểm chứng bởi `tests/test_features_no_leakage.py`.

## Feature selection

Xếp hạng độ quan trọng và danh sách feature đề nghị loại (importance share thấp) được sinh bởi
`model/feature_report.py` → `model/feature_importance_summary.json` (chạy sau khi train).
