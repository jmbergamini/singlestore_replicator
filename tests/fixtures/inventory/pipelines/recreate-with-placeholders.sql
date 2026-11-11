-- Every CREATE PIPELINE below carries <<CREDENTIALS>> in place of the credentials; fill it in before running.
-- Run inside the replacement database. Pipelines are created stopped. Never START here.

-- pipeline: orders_stream | state: Running | source: KAFKA | target: table orders
CREATE PIPELINE `orders_stream` AS LOAD DATA KAFKA 'broker-1:9092,broker-2:9092/orders' CONFIG '{"sasl.mechanism": "PLAIN"}' CREDENTIALS '<<CREDENTIALS>>' BATCH_INTERVAL 2500 REPLACE INTO TABLE `orders` FORMAT JSON;
ALTER PIPELINE `orders_stream` SET OFFSETS '{"0": 48000, "1": 47500}';

-- pipeline: imports | state: Stopped | source: S3 | target: table events
CREATE PIPELINE `imports` AS LOAD DATA S3 'example-bucket/imports/' CONFIG '{"region": "us-west-2"}' CREDENTIALS '<<CREDENTIALS>>' INTO TABLE `events` FORMAT CSV;
