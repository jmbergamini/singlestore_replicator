CREATE TABLE `orders` (
  `id` bigint(20) NOT NULL,
  `customer_id` bigint(20) NOT NULL,
  `status` varchar(20) CHARACTER SET utf8mb4 DEFAULT NULL,
  `amount_cents` bigint(20) DEFAULT NULL,
  `created_at` datetime(6) DEFAULT NULL,
  `updated_at` datetime(6) DEFAULT NULL,
  PRIMARY KEY (`id`),
  SHARD KEY (`id`),
  SORT KEY (`created_at`)
) AUTOSTATS_SAMPLING=ON
