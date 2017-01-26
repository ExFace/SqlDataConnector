<?php namespace exface\SqlDataConnector;

use exface\Core\CommonLogic\Model\Object;
use exface\Core\CommonLogic\NameResolver;
use exface\Core\Exceptions\UnexpectedValueException;
use exface\Core\Interfaces\InstallerInterface;
use exface\Core\CommonLogic\AbstractApp;
use exface\SqlDataConnector\SqlSchemaInstaller;
use exface\SqlDataConnector\Interfaces\SqlDataConnectorInterface;

class SqlDataConnectorApp extends AbstractApp {
	const FOLDER_WITH_MODEL_SOURCE_SQL = 'ModelSource';
	const FOLDER_WITH_MODEL_SOURCE_SQL_UPDATES = 'updates';
	
	private $data_types = NULL;
	private $explorer = NULL;
	
	public function get_attribute_properties_from_table(Object $meta_object, $table_name){
		// Determine the DB type
		if (!$this->get_explorer()){
			$name = $this->get_workbench()->create_name_resolver($meta_object->get_data_source()->get_data_connector_alias(), NameResolver::OBJECT_TYPE_DATA_CONNECTOR);
			switch ($name->get_alias()){
				case 'OracleSQL': $this->set_explorer(new OracleSQLExplorer($this)); break;
				case 'MySQL':
				case 'ModxDb': $this->set_explorer(new MySQLExplorer($this)); break;
				case 'MsSQL': $this->set_explorer(new MSSQLExplorer($this)); break;
				default: throw new UnexpectedValueException('Unsupported database type "' . $name->get_alias() . '"!', '6T5U304');
			}
		}
		return $this->get_explorer()->get_attribute_properties_from_table($meta_object, $table_name);
	}
	
	public function get_data_type_id($data_type_alias){
		if (!$this->data_types){
			$this->data_types = $this->get_workbench()->data()->create_data_sheet($this->get_workbench()->model()->get_object('exface.Core.DATATYPE'));
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
	
	public function get_installer(InstallerInterface $injected_installer = null){
		$installer = parent::get_installer($injected_installer);
		
		// First of all update the meta model source schema if the model is stored in SQL (= if the connector 
		// used by the model loader is an SQL connector). This needst ot be done before any other installers are run -
		// in particular before the model installer, because the model installer will attempt to write to the new
		// SQL schema already!
		if ($this->get_workbench()->model()->get_model_loader()->get_data_connection() instanceof SqlDataConnectorInterface) {
			$model_installer = new SqlSchemaInstaller($this->get_name_resolver());
			$model_installer->set_data_connection($this->get_workbench()->model()->get_model_loader()->get_data_connection());
			$installer->add_installer($model_installer, true);
		}
		return $installer;
	}
	
}
?>