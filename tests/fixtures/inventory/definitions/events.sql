CREATE TABLE `events` (
  `event_date` date DEFAULT NULL,
  `kind` varchar(50) DEFAULT NULL,
  `payload` JSON COLLATE utf8mb4_bin,
  `value` double DEFAULT NULL,
  SHARD KEY (`kind`),
  SORT KEY (`event_date`)
)
