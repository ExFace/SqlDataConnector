ALTER TABLE `exf_data_source` CHANGE `data_connection_oid` `custom_connection_oid` BINARY(16) NULL DEFAULT NULL;
ALTER TABLE `exf_data_source` ADD `default_connection_oid` BINARY(16) NULL AFTER `current_connection_oid`;
