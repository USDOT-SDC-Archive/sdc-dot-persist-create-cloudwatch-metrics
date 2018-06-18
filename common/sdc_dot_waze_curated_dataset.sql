DROP TABLE IF EXISTS alert;
CREATE TABLE alert (
  alert_uuid VARCHAR(100) encode zstd,
  alert_type VARCHAR(100) encode zstd,
  sub_type VARCHAR(100) encode zstd,
  street VARCHAR(500) encode zstd,
  city VARCHAR(500) encode zstd,
  state VARCHAR(10) encode zstd,
  country VARCHAR(100) encode zstd,
  num_thumbsup VARCHAR(100) encode zstd,
  reliability VARCHAR(100) encode zstd,
  confidence VARCHAR(100) encode zstd,
  report_rating VARCHAR(100) encode zstd,
  magvar VARCHAR(50) encode zstd,
  report_description VARCHAR(1000) encode zstd,
  location_lat VARCHAR(100) encode zstd,
  location_lon VARCHAR(100) encode zstd,
  jam_uuid VARCHAR(100) encode zstd,
  pub_millis VARCHAR(50) encode zstd,
  pub_utc_timestamp TIMESTAMP encode delta32k,
  pub_utc_epoch_week VARCHAR(100) encode zstd
)
distkey(alert_uuid)
sortkey(pub_utc_epoch_week,state,city,alert_type);

DROP TABLE IF EXISTS jam;
CREATE TABLE jam (
  id VARCHAR(100) encode zstd,
  jam_uuid VARCHAR(100) encode zstd,
  jam_type VARCHAR(100) encode zstd,
  road_type VARCHAR(100) encode zstd,
  street VARCHAR(500) encode zstd,
  city VARCHAR(500) encode zstd,
  state VARCHAR(10) encode zstd,
  country VARCHAR(100) encode zstd,
  speed VARCHAR(100) encode zstd,
  start_node VARCHAR(100) encode zstd,
  end_node VARCHAR(100) encode zstd,
  level VARCHAR(100) encode zstd,
  jam_length VARCHAR(100) encode zstd,
  delay VARCHAR(100) encode zstd,
  turn_line VARCHAR(100) encode zstd,
  turn_type VARCHAR(100) encode zstd,
  blocking_alert_uuid VARCHAR(100) encode zstd,
  pub_millis VARCHAR(100) encode zstd,
  pub_utc_timestamp TIMESTAMP encode delta32k,
  pub_utc_epoch_week VARCHAR(100) encode zstd
)
distkey(id)
sortkey(pub_utc_epoch_week, state, city, jam_type, level);

DROP TABLE IF EXISTS jam_point_sequence;
CREATE TABLE jam_point_sequence(
  jam_id VARCHAR(100) encode zstd,
  location_x VARCHAR(100) encode zstd,
  location_y VARCHAR(100) encode zstd,
  sequence_order VARCHAR(10) encode zstd
)
diststyle all;

DROP TABLE IF EXISTS irregularity;
CREATE TABLE irregularity(
  id VARCHAR(100) encode zstd,
  irregularity_type VARCHAR(100) encode zstd,
  street VARCHAR(500) encode zstd,
  city VARCHAR(500) encode zstd,
  state VARCHAR(10) encode zstd,
  country VARCHAR(100) encode zstd,
  speed VARCHAR(100) encode zstd,
  regular_speed VARCHAR(100) encode zstd,
  seconds VARCHAR(100) encode zstd,
  delay_seconds VARCHAR(100) encode zstd,
  trend VARCHAR(100) encode zstd,
  num_thumbsup VARCHAR(100) encode zstd,
  irregularity_length VARCHAR(100) encode zstd,
  severity VARCHAR(100) encode zstd,
  jam_level VARCHAR(100) encode zstd,
  drivers_count VARCHAR(100) encode zstd,
  alerts_count VARCHAR(100) encode zstd,
  num_comments VARCHAR(1000) encode zstd,
  highway VARCHAR(100) encode zstd,
  num_images VARCHAR(100) encode zstd,
  end_node VARCHAR(100) encode zstd,
  detection_date VARCHAR(100) encode zstd,
  detection_millis VARCHAR(100) encode zstd,
  detection_utc_timestamp TIMESTAMP encode delta32k,
  detection_utc_epoch_week VARCHAR(100) encode zstd,
  update_date VARCHAR(100) encode zstd,
  update_millis VARCHAR(100) encode zstd,
  update_utc_timestamp TIMESTAMP encode delta32k,
  update_utc_epoch_week VARCHAR(100) encode zstd
)
distkey(id)
sortkey(detection_utc_epoch_week, update_utc_epoch_week, state,city,irregularity_type);

DROP TABLE IF EXISTS irregularity_point_sequence;
CREATE TABLE irregularity_point_sequence(
  irregularity_id VARCHAR(100) encode zstd,
  location_x VARCHAR(100) encode zstd,
  location_y VARCHAR(100) encode zstd,
  sequence_order VARCHAR(10) encode zstd
)
diststyle all;

DROP TABLE IF EXISTS irregularity_alert;
CREATE TABLE irregularity_alert(
  irregularity_id VARCHAR(100) encode zstd,
  alert_uuid VARCHAR(100) encode zstd
)
diststyle all;

DROP TABLE IF EXISTS irregularity_jam;
CREATE TABLE irregularity_jam(
  irregularity_id VARCHAR(100) encode zstd,
  jam_uuid VARCHAR(100) encode zstd
)
diststyle all;

DROP TABLE IF EXISTS corridor_reading;
CREATE TABLE corridor_reading(
    corridor_id VARCHAR(100) encode zstd,
    partner_id VARCHAR(10) encode zstd,
    partner_name VARCHAR(100) encode zstd,
    city VARCHAR(500) encode zstd,
    state VARCHAR(10) encode zstd,
    corridor_name VARCHAR(1000) encode zstd,
    corridor_to_name VARCHAR(500) encode zstd,
    corridor_from_name VARCHAR(500) encode zstd,
    corridor_type VARCHAR(100) encode zstd,
    length VARCHAR(100) encode zstd,
    travel_time VARCHAR(100) encode zstd,
    historic_travel_time VARCHAR(100) encode zstd,
    delay VARCHAR(100) encode zstd,
    speed VARCHAR(100) encode zstd,
    historic_speed VARCHAR(100) encode zstd,
    bbox_minY VARCHAR(100) encode zstd,
    bbox_minX VARCHAR(100) encode zstd,
    bbox_maxY VARCHAR(100) encode zstd,
    bbox_maxX VARCHAR(100) encode zstd,
    update_millis VARCHAR(100) encode zstd,
    pub_utc_timestamp TIMESTAMP encode delta32k,
    pub_utc_epoch_week VARCHAR(10) encode zstd
)
distkey(corridor_id)
sortkey(pub_utc_epoch_week, state, city, partner_id);

DROP TABLE IF EXISTS corridor_point_sequence;
CREATE TABLE corridor_point_sequence(
    corridor_id VARCHAR(100) encode zstd,
    location_x VARCHAR(100) encode zstd,
    location_y VARCHAR(100) encode zstd,
    sequence_order VARCHAR(10) encode zstd
)
diststyle all;

DROP TABLE IF EXISTS corridor_alert;
CREATE TABLE corridor_alert(
    corridor_id VARCHAR(100) encode zstd,
    alert_uuid VARCHAR(100) encode zstd
)
diststyle all;

DROP TABLE IF EXISTS corridor_jam;
CREATE TABLE corridor_jam(
    corridor_id VARCHAR(100) encode zstd,
    jam_uuid VARCHAR(100) encode zstd
)
diststyle all;



