CREATE TABLE IF NOT EXISTS `exf_error` (
  `oid` binary(16) NOT NULL,
  `app_oid` binary(16) NOT NULL,
  `error_code` varchar(8) NOT NULL,
  `error_text` varchar(250) DEFAULT NULL,
  `created_on` datetime NOT NULL,
  `modified_on` datetime NOT NULL,
  `created_by_user_oid` binary(16) DEFAULT NULL,
  `modified_by_user_oid` binary(16) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE exf_error_code;

DROP TABLE exf_config;