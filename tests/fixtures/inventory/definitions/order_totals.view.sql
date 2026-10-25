CREATE DEFINER=`app_user`@`%` VIEW `order_totals` AS SELECT `customer_id`, SUM(`amount_cents`) AS `cents` FROM `orders` GROUP BY `customer_id`
