# Phản hồi: Kỹ thuật Dữ liệu Medallion

Anti-pattern (mẫu thiết kế lỗi) mà dữ liệu của nhóm chúng tôi có nguy cơ mắc phải cao nhất là **"Vấn đề file nhỏ" (Small-File Problem - Slide §5)**.

### Tại sao?
Trường hợp sử dụng của chúng tôi (quan sát LLM - LLM observability) liên quan đến việc nạp dữ liệu dạng streaming, trong đó các bản ghi thô (tầng Bronze) thường được ghi theo thời gian thực. Điều này tự nhiên dẫn đến việc hàng ngàn file Parquet cực nhỏ được tạo ra mỗi giờ. Như đã chứng minh trong bài **NB2**, việc có 200 file nhỏ cho chỉ 1 triệu dòng dữ liệu đã làm giảm đáng kể hiệu suất truy vấn điểm (point-query), với độ trễ trung bình lên tới hơn **110ms**.

Nếu không có sự quản lý chủ động, anti-pattern này sẽ gây ra:
*   **Quá tải Metadata:** Công cụ truy vấn tốn nhiều thời gian để mở/đóng file hơn là đọc dữ liệu thực tế.
*   **Thiếu khả năng lược bỏ (Pruning):** Dữ liệu bị phân tán ngẫu nhiên giữa các file, buộc hệ thống phải quét hầu hết mọi file ngay cả đối với một bộ lọc đơn giản như `user_id`.

### Giải pháp khắc phục
Bằng cách triển khai **`OPTIMIZE` (Nén file)** và **`Z-ORDER`** trên các cột có độ đa dạng cao như `user_id`, chúng tôi đã quan sát thấy tốc độ truy vấn tăng **8,5 lần** và tỷ lệ loại bỏ file đạt **55 lần**. Điều này xác nhận rằng một chiến lược bảo trì Lakehouse chủ động là thiết yếu để ngăn chặn kiến trúc Medallion của chúng tôi trở thành một "Đầm lầy dữ liệu" (Data Swamp).