<?php namespace exface\SqlDataConnector;

use exface\Core\CommonLogic\Model\Object;

abstract class AbstractSQLExplorer {
	private $app;
	
	public function __construct(SqlDataConnectorApp $app){
		$this->app = $app;
	}
	
	public function get_app() {
		return $this->app;
	}
	
	abstract public function get_attribute_properties_from_table(Object $meta_object, $table_name);
	
	/**
	 * 
	 * @param string $sql_data_type
	 * @param integer $length total number of digits/characters
	 * @param integer $number_scale number of digits to the right of the decimal point
	 */
	public function get_data_type($sql_data_type, $length = null, $number_scale = null){
		$data_type_alias = '';
		switch (strtoupper($sql_data_type)){
			case 'NUMBER': 
			case 'BIGINT':
			case 'INT':
			case 'INTEGER':
			case 'DECIMAL':
			case 'FLOAT': $data_type_alias = 'Number'; break;
			case 'TIMESTAMP': 
			case 'DATETIME': $data_type_alias = 'Timestamp'; break;
			case 'DATE': $data_type_alias = 'Date'; break;
			case 'TEXT': case 'LONGTEXT': $data_type_alias = 'Text'; break;
			default: $data_type_alias = 'String';
		}
		return $data_type_alias;
	}
	
	/**
	 * Prettifies the column name to be used as the attribute's label
	 * @param string $column_name
	 * @return string
	 */
	public function generate_label($column_name){
		$column_name = trim($column_name);
		$column_name = str_replace('_', ' ', $column_name);
		$column_name = strtolower($column_name);
		$column_name = ucfirst($column_name);
		return $column_name;
	}
}
?>