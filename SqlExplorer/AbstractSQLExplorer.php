<?php namespace exface\SqlDataConnector\SqlExplorer;

use exface\Core\CommonLogic\Model\Object;
use exface\SqlDataConnector\Interfaces\SqlDataConnectorInterface;
use exface\SqlDataConnector\Interfaces\SqlExplorerInterface;
use exface\Core\Factories\DataSheetFactory;


abstract class AbstractSQLExplorer implements SqlExplorerInterface {
	private $data_connector = null;
	private $data_types = NULL;
	
	public function __construct(SqlDataConnectorInterface $data_connector){
		$this->data_connector = $data_connector;
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
	
	public function get_data_type_id($data_type_alias){
		if (!$this->data_types){
			$this->data_types = DataSheetFactory::create_from_object($this->get_data_connection()->get_workbench()->model()->get_object('exface.Core.DATATYPE'));
			$this->data_types->get_columns()->add_multiple(array($this->data_types->get_meta_object()->get_uid_alias(), $this->data_types->get_meta_object()->get_label_alias()));
			$this->data_types->data_read(0, 0);
		}
	
		return $this->data_types->get_uid_column()->get_cell_value($this->data_types->get_columns()->get('LABEL')->find_row_by_value($data_type_alias));
	}
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\SqlDataConnector\Interfaces\SqlExplorerInterface::get_data_connection()
	 */
	public function get_data_connection(){
		return $this->data_connector;
	}
}
?>