CREATE TABLE `totals` (
  `day` date NOT NULL,
  `orders` int(11) DEFAULT NULL,
  `cents` bigint(20) DEFAULT NULL,
  `dollars` as `cents` / 100 PERSISTED decimal(20,2),
  PRIMARY KEY (`day`)
)
