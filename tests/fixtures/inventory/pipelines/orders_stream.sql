-- pipeline: orders_stream | state: Running | source: KAFKA | target: table orders
CREATE PIPELINE `orders_stream` AS LOAD DATA KAFKA 'broker-1:9092,broker-2:9092/orders' CONFIG '{"sasl.mechanism": "PLAIN"}' CREDENTIALS '<<CREDENTIALS>>' BATCH_INTERVAL 2500 REPLACE INTO TABLE `orders` FORMAT JSON;
ALTER PIPELINE `orders_stream` SET OFFSETS '{"0": 48000, "1": 47500}';
