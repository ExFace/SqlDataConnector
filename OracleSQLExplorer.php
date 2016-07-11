<?php namespace exface\SqlDataConnector;

use exface\Core\CommonLogic\Model\Object;

class OracleSQLExplorer extends AbstractSQLExplorer {
	
	public function get_attribute_properties_from_table(Object $meta_object, $table_name){
		$columns_sql = "
					SELECT
						tc.column_name,
						tc.nullable,
						tc.data_type,
						tc.data_precision,
						tc.data_scale,
						tc.data_length,
						cc.comments
					FROM user_col_comments cc
						JOIN user_tab_columns tc ON cc.column_name = tc.column_name AND cc.table_name = tc.table_name
					WHERE UPPER(cc.table_name) = UPPER('" . $table_name . "')
				";
			
		// TODO check if it is the right data connector
		$columns_array = $meta_object->get_data_connection()->query($columns_sql);
		$rows = array();
		foreach ($columns_array as $col){		
			$rows[] = array(
					'LABEL' => $this->generate_label($col['COLUMN_NAME']),
					'ALIAS' => $col['COLUMN_NAME'],
					'DATATYPE' => $this->get_app()->get_data_type_id($this->get_data_type($col['DATA_TYPE'], ($col['DATA_PRECISION'] ? $col['DATA_PRECISION'] : $col['DATA_LENGTH']), $col['DATA_SCALE'])),
					'DATA_ADDRESS' => $col['COLUMN_NAME'],
					'OBJECT' => $meta_object->get_id(),
					'REQUIREDFLAG' => ($col['NULLABLE'] == 'N' ? 1 : 0),
					'SHORT_DESCRIPTION' => ($col['COMMENTS'] ? $col['COMMENTS'] : ''),
			);
		}
		return $rows;
	}	
}
?>