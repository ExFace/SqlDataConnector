<?php namespace exface\SqlDataConnector;

use exface\Core\CommonLogic\Model\Object;

class MySQLExplorer extends AbstractSQLExplorer {
	
	public function get_attribute_properties_from_table(Object $meta_object, $table_name){
		$columns_sql = "
					SHOW FULL COLUMNS FROM " . $table_name . "
				";
			
		// TODO check if it is the right data connector
		$columns_array = $meta_object->get_data_connection()->run_sql($columns_sql)->get_result_array();
		$rows = array();
		foreach ($columns_array as $col){		
			$rows[] = array(
					'LABEL' => $this->generate_label($col['Field']),
					'ALIAS' => $col['Field'],
					'DATATYPE' => $this->get_app()->get_data_type_id($this->get_data_type($col['Type'])),
					'DATA_ADDRESS' => $col['Field'],
					'OBJECT' => $meta_object->get_id(),
					'REQUIREDFLAG' => ($col['Null'] == 'NO' ? 1 : 0),
					'SHORT_DESCRIPTION' => ($col['Comment'] ? $col['Comment'] : ''),
			);
		}
		return $rows;
	}	
	
	public function get_data_type($data_type, $length = null, $number_scale = null){
		$data_type = trim($data_type);
		$details = array();
		$type = substr($data_type, strpos($data_type, '('));
		if (strpos($data_type, '(') !== false){
			$details = explode(',', substr($data_type, (strpos($data_type, '('))+1, (strlen($data_type) - strrpos($data_type, ')'))));
		}
		
		return parent::get_data_type($type, $details[0], $details[1]);
	}
}
?>