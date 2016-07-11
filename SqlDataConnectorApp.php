<?php namespace exface\SqlDataConnector;

use exface\Core\CommonLogic\Model\Object;
use exface\Core\Exceptions\ActionRuntimeException;
use exface\Core\CommonLogic\NameResolver;

class SqlDataConnectorApp extends \exface\Core\CommonLogic\AbstractApp {
	private $data_types = NULL;
	private $explorer = NULL;
	
	public function get_attribute_properties_from_table(Object $meta_object, $table_name){
		// Determine the DB type
		if (!$this->get_explorer()){
			$name = $this->exface()->create_name_resolver($meta_object->get_data_source()->get_data_connector_alias(), NameResolver::OBJECT_TYPE_DATA_CONNECTOR);
			switch ($name->get_alias()){
				case 'OracleSQL': $this->set_explorer(new OracleSQLExplorer($this)); break;
				case 'MySQL':
				case 'ModxDb': $this->set_explorer(new MySQLExplorer($this)); break;
				case 'MsSQL': $this->set_explorer(new MSSQLExplorer($this)); break;
				default: throw new ActionRuntimeException('Unsupported database type "' . $name->get_alias() . '"!');
			}
		}
		return $this->get_explorer()->get_attribute_properties_from_table($meta_object, $table_name);
	}
	
	public function get_data_type_id($data_type_alias){
		if (!$this->data_types){
			$this->data_types = $this->exface()->data()->create_data_sheet($this->exface()->model()->get_object('exface.Core.DATATYPE'));
			$this->data_types->get_columns()->add_multiple(array($this->data_types->get_meta_object()->get_uid_alias(), $this->data_types->get_meta_object()->get_label_alias()));
			$this->data_types->data_read(0, 0);
		}
		
		return $this->data_types->get_uid_column()->get_cell_value($this->data_types->get_columns()->get('LABEL')->find_row_by_value($data_type_alias));
	}
	
	public function get_explorer() {
		return $this->explorer;
	}
	
	public function set_explorer(AbstractSQLExplorer $explorer) {
		$this->explorer = $explorer;
		return $this;
	}
}
?>