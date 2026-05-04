from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, when, current_timestamp
from pyspark.sql.types import StringType
import hashlib

# 1. Khởi tạo Spark với Delta Lake
spark = SparkSession.builder \
    .appName("CDC_RideHailing_PoC") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .get_name_or_create()

# 2. Cơ chế Tokenization (Hàm băm bảo mật cho PII - Số điện thoại)
# Trong thực tế, bạn sẽ dùng một KMS hoặc Vault để lấy Salt
def tokenize_phone(phone):
    if phone is None: return None
    salt = "DECREE13_SALT_2024"
    return hashlib.sha256((phone + salt).encode()).hexdigest()

tokenize_udf = udf(tokenize_phone, StringType())

# 3. Tạo dữ liệu giả lập từ Kafka (Bronze Layer - CDC Stream)
# Giả sử có 2 bản tin: 1 tạo mới chuyến đi, 1 cập nhật trạng thái nhưng đến muộn
cdc_data = [
    ("trip_001", "0901234567", "CREATED", "2024-05-20 10:00:00"), # Bản tin gốc
    ("trip_001", "0901234567", "COMPLETED", "2024-05-20 10:15:00") # Bản tin cập nhật
]
columns = ["trip_id", "phone_number", "status", "event_ts"]
raw_df = spark.createDataFrame(cdc_data, columns) \
    .withColumn("event_ts", col("event_ts").cast("timestamp"))

# 4. Thực hiện Tokenization ngay tại tầng Bronze
bronze_df = raw_df.withColumn("tokenized_phone", tokenize_udf(col("phone_number"))) \
                  .drop("phone_number") # Xóa số điện thoại thô để tuân thủ Nghị định 13

# 5. Upsert vào Silver Layer dùng MERGE (Xử lý Late-arriving data)
# Giả sử bảng Silver đã tồn tại
bronze_df.createOrReplaceTempView("bronze_updates")

# Logic MERGE: Chỉ cập nhật nếu event_ts của dữ liệu mới lớn hơn dữ liệu cũ
spark.sql("""
    MERGE INTO trips_silver AS target
    USING bronze_updates AS source
    ON target.trip_id = source.trip_id
    WHEN MATCHED AND source.event_ts > target.event_ts THEN
      UPDATE SET 
        target.status = source.status,
        target.event_ts = source.event_ts,
        target.updated_at = current_timestamp()
    WHEN NOT MATCHED THEN
      INSERT (trip_id, tokenized_phone, status, event_ts, updated_at)
      VALUES (source.trip_id, source.tokenized_phone, source.status, source.event_ts, current_timestamp())
""")

print("PoC: Đã xử lý Tokenization và MERGE thành công cho dữ liệu CDC.")