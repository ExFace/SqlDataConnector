<?php namespace exface\SqlDataConnector;

use exface\Core\CommonLogic\Model\Object;

class MSSQLExplorer extends AbstractSQLExplorer {
	
	public function get_attribute_properties_from_table(Object $meta_object, $table_name){
		$table_name_parts = explode('.', $table_name);
		if (count($table_name_parts) == 2) {
			$columns_sql = "
					exec sp_columns '" . $table_name_parts[1] . "', '" . $table_name_parts[0] . "'
				";
		} else {
			$columns_sql = "
					exec sp_columns '" . $table_name . "'
				";
		}
		
		// TODO check if it is the right data connector
		$columns_array = $meta_object->get_data_connection()->run_sql($columns_sql)->get_result_array();
		$rows = array();
		foreach ($columns_array as $col){		
			$rows[] = array(
					'LABEL' => $this->generate_label($col['COLUMN_NAME']),
					'ALIAS' => $col['COLUMN_NAME'],
					'DATATYPE' => $this->get_app()->get_data_type_id($this->get_data_type($col['TYPE_NAME'], $col['PRECISION'], $col['SCALE'])),
					'DATA_ADDRESS' => $col['COLUMN_NAME'],
					'OBJECT' => $meta_object->get_id(),
					'REQUIREDFLAG' => ($col['NULLABLE'] == 0 ? 1 : 0),
					'DEFAULT_VALUE' => (!is_null($col['COLUMN_DEF']) ? $col['COLUMN_DEF'] : '')
			);
		}
		return $rows;
	}		
}
?>