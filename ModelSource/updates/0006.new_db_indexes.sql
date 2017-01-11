/* Delete junk dublicate object from early tests to prevent dublicate key errors */
DELETE FROM `exf_object` WHERE oid = 0x38300000000000000000000000000000;

/* Add indexes to object table */
ALTER TABLE `exf_object` ADD INDEX `app_oid` (`app_oid`) USING BTREE;
ALTER TABLE `exf_object` ADD INDEX `parent_object_oid` (`parent_object_oid`) USING BTREE;
ALTER TABLE `exf_object` ADD INDEX `data_source_oid` (`data_source_oid`) USING BTREE;
ALTER TABLE `exf_object` ADD UNIQUE `alias+app_oid` (`object_alias`, `app_oid`) USING BTREE;

/* Add indexes to attribute table*/
ALTER TABLE `exf_attribute` ADD INDEX `object_oid` (`object_oid`) USING BTREE;
ALTER TABLE `exf_attribute` ADD INDEX `related_object_oid` (`related_object_oid`) USING BTREE;