<?php
namespace exface\SqlDataConnector;

use exface\Core\CommonLogic\AbstractApp;
use exface\SqlDataConnector\Interfaces\SqlDataConnectorInterface;
use exface\Core\CommonLogic\AbstractAppInstaller;
use exface\Core\Factories\ConfigurationFactory;

/**
 * This installer creates and manages an SQL schema for an ExFace app.
 * 
 * If the app has it's own SQL database (= is not built on top of an existing database), changes to the SQL schema must go
 * hand-in-hand with changes of the meta model and the code. This installer takes care of updating the schema by performing
 * SQL scripts stored in a specifal folder within the app (by default "install/sql"). These scripts must follow a simple
 * naming convention: they start with a number followed by a dot and a textual description. Update scripts are executed
 * in the order of the leading number. The number of the last script executed is stored in the installation scope of the
 * app's config, so the next time the installer runs, only new updates will get executed.
 *  
 * @author Andrej Kabachnik
 *
 */
class SqlSchemaInstaller extends AbstractAppInstaller {
	
	private $sql_folder_name = 'sql';
	private $sql_updates_folder_name = 'updates';
	private $sql_install_folder_name = 'install';
	private $sql_uninstall_folder_name = 'uninstall';
	private $data_connection = null;
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\Core\CommonLogic\AbstractApp::install()
	 */
	public function install($source_absolute_path){
		return $this->update($source_absolute_path);
	}
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\Core\Interfaces\InstallerInterface::update()
	 */
	public function update($source_absolute_path){
		return $this->perform_model_source_update($source_absolute_path);
	}
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\Core\Interfaces\InstallerInterface::uninstall()
	 */
	public function uninstall(){
		// Remove index-exface.php to the root of the MODx installation
		$this->get_workbench()->filemanager()->remove($this->get_app()->get_modx_ajax_index_path());
		return "\nRemoved " . $this->get_app()->get_modx_ajax_index_path() . '.'; 
	}
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\Core\Interfaces\InstallerInterface::backup()
	 */
	public function backup($destination_absolute_path){
		return 'Backup not implemented for' . $this->get_app_name_resolver()->get_alias_with_namespace() . '!';
	}
	
	public function get_sql_folder_name() {
		return $this->sql_folder_name;
	}
	
	public function set_sql_folder_name($value) {
		$this->sql_folder_name = $value;
		return $this;
	}
	
	/**
	 * Default: %app_folder%/Install/sql
	 * 
	 * @return string
	 */
	public function get_sql_folder_absolute_path($source_absolute_path){
		return $this->get_install_folder_absolute_path($source_absolute_path) . DIRECTORY_SEPARATOR . $this->get_sql_folder_name();
	}
	
	public function get_sql_install_folder_name() {
		return $this->sql_install_folder_name;
	}
	
	public function set_sql_install_folder_name($value) {
		$this->sql_install_folder_name = $value;
		return $this;
	}
	
	public function get_sql_updates_folder_name() {
		return $this->sql_updates_folder_name;
	}
	
	public function set_sql_updates_folder_name($value) {
		$this->sql_updates_folder_name = $value;
		return $this;
	}
	
	public function get_sql_uninstall_folder_name() {
		return $this->sql_uninstall_folder_name;
	}
	
	public function set_sql_uninstall_folder_name($value) {
		$this->sql_uninstall_folder_name = $value;
		return $this;
	}  
	
	public function get_data_connection() {
		return $this->data_connection;
	}
	
	public function set_data_connection(SqlDataConnectorInterface $value) {
		$this->data_connection = $value;
		return $this;
	}
	
	/**
	 * Iterates through the files in "%source_absolute_path%/%install_folder_name%/%sql_folder_name%/%sql_updates_folder_name%" (by default
	 * "%source_absolute_path%/Install/sql/updates") attempting to execute the SQL scripts stored in those files. If anything goes wrong, 
	 * all subsequent files are not executed and the last successfull update is marked as performed. Thus, once the update is triggered 
	 * again, it will try to perform all the updates starting from the failed one.
	 * 
	 * In order to explicitly skip one or more update files, increase the option LAST_PERFORMED_MODEL_SOURCE_UPDATE_ID in the local config
	 * file of the app being installed to match the last update number that should not be installed. 
	 * 
	 * @param string $source_absolute_path
	 * @return string
	 */
	protected function perform_model_source_update($source_absolute_path){
		
		$updates_folder = $this->get_sql_folder_absolute_path($source_absolute_path) . DIRECTORY_SEPARATOR . $this->get_sql_updates_folder_name();
		$id_installed = $this->get_app()->get_config()->get_option('LAST_PERFORMED_MODEL_SOURCE_UPDATE_ID');
		$updates_installed = array();
		$updates_failed = array();
		
		foreach (scandir($updates_folder, SCANDIR_SORT_ASCENDING) as $file){
			if ($file == '.' || $file == '..') continue;
			$id = intval(substr($file, 0, 4));
			if ($id > $id_installed){
				if (count($updates_failed) > 1){
					$updates_failed[] = $id;
					continue;
				}
				$sql = file_get_contents($updates_folder . DIRECTORY_SEPARATOR . $file);
				$sql = trim(preg_replace('/\s+/', ' ', $sql));
				try {
					foreach (explode(';', $sql) as $statement){
						if ($statement){
							$this->get_data_connection()->run_sql($statement);
						}
					}
					$updates_installed[] = $id;
				} catch (\Throwable $e){
					$updates_failed[] = $id;
				} finally {
					$updates_failed[] = $id;
				}
			}
				
		}
		// Save the last id in order to skip installed ones next time
		if (count($updates_installed) > 0){
			$this->set_last_model_source_update_id(end($updates_installed));
		}
			
		$result = "\n";
		if (count($updates_installed) > 0){
			$result .= 'Installed ' . count($updates_installed) . ' model source updates';
		}
		if (count($updates_failed) > 0){
			$result .= ' (' . count($updates_failed) . ' updates failed)';
		}
	
		return $result;
	}
	
	protected function set_last_model_source_update_id($id){
		$filename = $this->get_workbench()->filemanager()->get_path_to_config_folder() . DIRECTORY_SEPARATOR . $this->get_app()->get_config_file_name();
	
		// Load the installation specific config file
		$config = ConfigurationFactory::create($this->get_workbench());
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