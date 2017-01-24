/* Create the table */
DROP TABLE IF EXISTS `exf_object_actions`;
DROP TABLE IF EXISTS `exf_object_action`;
CREATE TABLE `exf_object_action` (
  `oid` binary(16) NOT NULL,
  `object_oid` binary(16) NOT NULL,
  `action` varchar(128) NOT NULL,
  `alias` varchar(128) NOT NULL,
  `name` varchar(128) DEFAULT NULL,
  `short_description` text,
  `config_uxon` text,
  `action_app_oid` binary(16) NOT NULL,
  `use_in_object_basket_flag` tinyint(1) NOT NULL DEFAULT '0',
  `created_on` datetime NOT NULL,
  `modified_on` datetime NOT NULL,
  `created_by_user_oid` binary(16) DEFAULT NULL,
  `modified_by_user_oid` binary(16) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/* Add indexes */
ALTER TABLE `exf_object_action`
  ADD PRIMARY KEY (`oid`),
  ADD KEY `object_oid` (`object_oid`) USING BTREE;
