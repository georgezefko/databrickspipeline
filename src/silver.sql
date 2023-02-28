CREATE TABLE IF NOT EXISTS silver.bookings_silver (
  pageurl STRING,
  uniq_id STRING,
  hotel_id INT NOT NULL,
  hotel_name STRING NOT NULL,
  ota STRING,
  crawled_date TIMESTAMP,
  checkin_date DATE,
  room_type_breakfast STRING,
  room_type_cancellation STRING,
  room_type_name STRING,
  room_type_occupancy BIGINT,
  room_type_price FLOAT
)
USING delta
LOCATION 'dbfs:/user/hive/warehouse/silver/bookings_silver'
OPTIONS (
  PRIMARY_KEY 'uniq_id'
)