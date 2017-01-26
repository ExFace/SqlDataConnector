UPDATE `exf_data_connection` SET `data_connector` = replace(data_connector, 'Apps\\', '');
UPDATE `exf_data_connection` SET `data_connector` = replace(data_connector,  '\\', '/');

UPDATE `exf_data_source` SET `default_query_builder` = replace(default_query_builder, 'Apps\\', '');
UPDATE `exf_data_source` SET `default_query_builder` = replace(default_query_builder, '\\', '/');

UPDATE `exf_object_behaviors` SET `behavior` = replace(behavior, 'Apps\\', '');
UPDATE `exf_object_behaviors` SET `behavior` = replace(behavior, '\\', '/');