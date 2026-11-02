--- Publication cho Debezium (pgoutput)
-- Giữ REPLICA IDENTITY mặc định (khoá chính): event DELETE chỉ cần khoá chính để StarRocks xoá đúng dòng,
-- không cần before-image đầy đủ -> WAL nhỏ hơn so với REPLICA IDENTITY FULL.
create publication dbz_publication for table daily_bar, instrument, corporate_action;
