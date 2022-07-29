CREATE TABLE `cotacao_moedas` (
  `code` varchar(100) DEFAULT NULL,
  `codein` varchar(100) DEFAULT NULL,
  `name` varchar(150) DEFAULT NULL,
  `high` float DEFAULT NULL,
  `low` float DEFAULT NULL,
  `varBid` float DEFAULT NULL,
  `pctChange` float DEFAULT NULL,
  `bid` float DEFAULT NULL,
  `ask` float DEFAULT NULL,
  `timestamp` varchar(100) DEFAULT NULL,
  `created_date` datetime DEFAULT NULL
);