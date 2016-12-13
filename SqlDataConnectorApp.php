<?php namespace exface\SqlDataConnector;

use exface\Core\CommonLogic\Model\Object;
use exface\Core\Exceptions\ActionRuntimeException;
use exface\Core\CommonLogic\NameResolver;
use exface\Core\Factories\ConfigurationFactory;
use exface\SqlDataConnector\Interfaces\SqlDataConnectorInterface;

class SqlDataConnectorApp extends \exface\Core\CommonLogic\AbstractApp {
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
				default: throw new ActionRuntimeException('Unsupported database type "' . $name->get_alias() . '"!');
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
	
	public function install(){
		return $this->perform_model_source_update();
	}
	
	private function perform_model_source_update(){
		// This only works if the connector used by the model loader is an SQL connector
		if (!($this->get_workbench()->model()->get_model_loader()->get_data_connection() instanceof SqlDataConnectorInterface)) return;
		
		$updates_folder = __DIR__ . DIRECTORY_SEPARATOR . $this::FOLDER_WITH_MODEL_SOURCE_SQL . DIRECTORY_SEPARATOR . $this::FOLDER_WITH_MODEL_SOURCE_SQL_UPDATES;
		$id_installed = $this->get_config()->get_option('LAST_PERFORMED_MODEL_SOURCE_UPDATE_ID');
		$updates_installed = array();
		$updates_failed = array();
		
		foreach (scandir($updates_folder, SCANDIR_SORT_ASCENDING) as $file){
			if ($file == '.' || $file == '..') continue;
			$id = intval(substr($file, 0, 4));
			if ($id > $id_installed){
				$sql = file_get_contents($updates_folder . DIRECTORY_SEPARATOR . $file);
				$sql = trim(preg_replace('/\s+/', ' ', $sql));
				try {
					foreach (explode(';', $sql) as $statement){
						if ($statement){
							$this->get_workbench()->model()->get_model_loader()->get_data_connection()->run_sql($statement);
						}
					}
					$updates_installed[] = $id;
				} catch (\Exception $e){
					$updates_failed[] = $id;
				}
			}
			
		}
		// Save the last id in order to skip installed ones next time
		$this->set_last_model_source_update_id($id);
			
		$result = '';
		if (count($updates_installed) > 0){
			$result .= 'Installed ' . count($updates_installed) . ' model source updates';
		}
		if (count($updates_failed) > 0){
			$result .= ' (' . count($updates_failed) . ' updates failed)';
		}
		
		return $result;
	}
	
	private function set_last_model_source_update_id($id){
		$exface = $this->get_workbench();
		$filename = $this->get_workbench()->filemanager()->get_path_to_config_folder() . DIRECTORY_SEPARATOR . $this->get_config_file_name();
		
		// Load the installation specific config file
		$config = ConfigurationFactory::create($exface);
		if (file_exists($filename)){
			$config->load_config_file($filename);
		} 
		// Overwrite the option
		$config->set_option('LAST_PERFORMED_MODEL_SOURCE_UPDATE_ID', $id);
		// Save the file or create one if there was no installation specific config before
		file_put_contents($filename, $config->export_uxon_object()->to_json(true));
		
		return $this;
	}
}
?>