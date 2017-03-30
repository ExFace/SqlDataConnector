<?php namespace exface\SqlDataConnector\SqlExplorer;

use exface\Core\CommonLogic\Model\Object;

class SapHanaSQLExplorer extends AbstractSQLExplorer {
	
	public function get_attribute_properties_from_table(Object $meta_object, $table_name){
		$columns_sql = "SELECT * FROM TABLE_COLUMNS WHERE SCHEMA_NAME = 'SCM' AND TABLE_NAME = '" . $table_name . "' ORDER BY POSITION";
			
		// TODO check if it is the right data connector
		$columns_array = $meta_object->get_data_connection()->run_sql($columns_sql)->get_result_array();
		$rows = array();
		foreach ($columns_array as $col){		
			$rows[] = array(
					'LABEL' => $this->generate_label($col['COLUMN_NAME']),
					'ALIAS' => $col['COLUMN_NAME'],
					'DATATYPE' => $this->get_data_type_id($this->get_data_type($col['DATA_TYPE_NAME'])),
					'DATA_ADDRESS' => $col['COLUMN_NAME'],
					'OBJECT' => $meta_object->get_id(),
					'REQUIREDFLAG' => ($col['IS_NULLABLE'] == 'FALSE' ? 1 : 0),
					'SHORT_DESCRIPTION' => ($col['COMMENTS'] ? $col['COMMENTS'] : ''),
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