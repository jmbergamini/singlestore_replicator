CREATE TABLE `order_items` (
  `order_id` bigint(20) NOT NULL,
  `line_no` int(11) NOT NULL,
  `sku` varchar(40) DEFAULT NULL,
  `qty` int(11) DEFAULT NULL,
  `price_cents` bigint(20) DEFAULT NULL,
  `_modified_time` datetime(6) DEFAULT NULL,
  PRIMARY KEY (`order_id`,`line_no`),
  SHARD KEY (`order_id`),
  SORT KEY (`order_id`,`line_no`)
)
