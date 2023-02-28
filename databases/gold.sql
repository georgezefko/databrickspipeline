CREATE TABLE IF NOT EXISTS gold.revenue (
 
  hotel_id INT NOT NULL,
  hotel_name STRING NOT NULL,
  revenue int
)
USING delta
LOCATION 'dbfs:/user/hive/warehouse/gold/revenue'
OPTIONS (
  PRIMARY_KEY 'hotel_id'
)

CREATE TABLE IF NOT EXISTS gold.month_quarter_demand (
 
  hotel_id INT NOT NULL,
  hotel_name STRING NOT NULL,
  bookings INT,
  month INT,
  quarter STRING,
  month_quarter STRING,
  demand int
)
USING delta
LOCATION 'dbfs:/user/hive/warehouse/gold/month_quarter_demand'
OPTIONS (
  PRIMARY_KEY 'hotel_id'
)

CREATE TABLE IF NOT EXISTS gold.rooms_overview(
 
  hotel_id INT NOT NULL,
  hotel_name STRING NOT NULL,
  breakfast STRING,
  cancelation STRING,
  room_name STRING,
  avg_price INT,
  avg_num_persons INT,
  bookings INT,
  avg_days_dif INT
)
USING delta
LOCATION 'dbfs:/user/hive/warehouse/gold/rooms_overview'
OPTIONS (
  PRIMARY_KEY 'hotel_id'
)