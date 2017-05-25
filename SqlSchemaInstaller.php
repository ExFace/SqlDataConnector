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
class SqlSchemaInstaller extends AbstractAppInstaller
{

    private $sql_folder_name = 'sql';

    private $sql_updates_folder_name = 'updates';

    private $sql_install_folder_name = 'install';

    private $sql_uninstall_folder_name = 'uninstall';

    private $data_connection = null;

    /**
     *
     * {@inheritdoc}
     *
     * @see \exface\Core\CommonLogic\AbstractApp::install()
     */
    public function install($source_absolute_path)
    {
        return $this->update($source_absolute_path);
    }

    /**
     *
     * {@inheritdoc}
     *
     * @see \exface\Core\Interfaces\InstallerInterface::update()
     */
    public function update($source_absolute_path)
    {
        return $this->performModelSourceUpdate($source_absolute_path);
    }

    /**
     *
     * {@inheritdoc}
     *
     * @see \exface\Core\Interfaces\InstallerInterface::uninstall()
     */
    public function uninstall()
    {
        return 'Automatic uninstaller not implemented for' . $this->getNameResolver()->getAliasWithNamespace() . '!';
    }

    /**
     *
     * {@inheritdoc}
     *
     * @see \exface\Core\Interfaces\InstallerInterface::backup()
     */
    public function backup($destination_absolute_path)
    {
        return 'SQL Backup not implemented for ' . $this->getNameResolver()->getAliasWithNamespace() . '!';
    }

    /**
     *
     * @return string
     */
    public function getSqlFolderName()
    {
        return $this->sql_folder_name;
    }

    /**
     *
     * @param
     *            $value
     * @return $this
     */
    public function setSqlFolderName($value)
    {
        $this->sql_folder_name = $value;
        return $this;
    }

    /**
     * Default: %app_folder%/Install/sql
     *
     * @return string
     */
    public function getSqlFolderAbsolutePath($source_absolute_path)
    {
        return $this->getInstallFolderAbsolutePath($source_absolute_path) . DIRECTORY_SEPARATOR . $this->getSqlFolderName();
    }

    /**
     *
     * @return string
     */
    public function getSqlInstallFolderName()
    {
        return $this->sql_install_folder_name;
    }

    /**
     *
     * @param
     *            $value
     * @return $this
     */
    public function setSqlInstallFolderName($value)
    {
        $this->sql_install_folder_name = $value;
        return $this;
    }

    /**
     *
     * @return string
     */
    public function getSqlUpdatesFolderName()
    {
        return $this->sql_updates_folder_name;
    }

    /**
     *
     * @param
     *            $value
     * @return $this
     */
    public function setSqlUpdatesFolderName($value)
    {
        $this->sql_updates_folder_name = $value;
        return $this;
    }

    /**
     *
     * @return string
     */
    public function getSqlUninstallFolderName()
    {
        return $this->sql_uninstall_folder_name;
    }

    /**
     *
     * @param
     *            $value
     * @return $this
     */
    public function setSqlUninstallFolderName($value)
    {
        $this->sql_uninstall_folder_name = $value;
        return $this;
    }

    /**
     *
     * @return SqlDataConnectorInterface
     */
    public function getDataConnection()
    {
        return $this->data_connection;
    }

    /**
     *
     * @param SqlDataConnectorInterface $value            
     * @return $this
     */
    public function setDataConnection(SqlDataConnectorInterface $value)
    {
        $this->data_connection = $value;
        return $this;
    }

    /**
     * Iterates through the files in "%source_absolute_path%/%install_folder_name%/%sql_folder_name%/%sql_updates_folder_name%" (by default
     * "%source_absolute_path%/Install/sql/updates") attempting to execute the SQL scripts stored in those files.
     * If anything goes wrong,
     * all subsequent files are not executed and the last successfull update is marked as performed. Thus, once the update is triggered
     * again, it will try to perform all the updates starting from the failed one.
     *
     * In order to explicitly skip one or more update files, increase the option LAST_PERFORMED_MODEL_SOURCE_UPDATE_ID in the local config
     * file of the app being installed to match the last update number that should not be installed.
     *
     * @param string $source_absolute_path            
     * @return string
     */
    protected function performModelSourceUpdate($source_absolute_path)
    {
        $updates_folder = $this->getSqlFolderAbsolutePath($source_absolute_path) . DIRECTORY_SEPARATOR . $this->getSqlUpdatesFolderName();
        $id_installed = $this->getApp()->getConfig()->getOption('LAST_PERFORMED_MODEL_SOURCE_UPDATE_ID');
        
        $updates_installed = array();
        $updates_failed = array();
        $error_text = '';
        
        $updateFolderDirScan = array_diff(scandir($updates_folder, SCANDIR_SORT_ASCENDING), array(
            '..',
            '.'
        ));
        foreach ($updateFolderDirScan as $file) {
            $id = intval(substr($file, 0, 4));
            if ($id > $id_installed) {
                if (count($updates_failed) > 0) {
                    $updates_failed[] = $id;
                    continue;
                }
                $sql = file_get_contents($updates_folder . DIRECTORY_SEPARATOR . $file);
                // $sql = trim(preg_replace('/\s+/', ' ', $sql));
                try {
                    $this->getDataConnection()->transactionStart();
                    foreach (preg_split("/;\R/", $sql) as $statement) {
                        if ($statement) {
                            $this->getDataConnection()->runSql($statement);
                        }
                    }
                    $this->getDataConnection()->transactionCommit();
                    $updates_installed[] = $id;
                } catch (\Throwable $e) {
                    $updates_failed[] = $id;
                    $error_text = $e->getMessage();
                }
            }
        }
        // Save the last id in order to skip installed ones next time
        if (count($updates_installed) > 0) {
            $this->setLastModelSourceUpdateId(end($updates_installed));
        }
        
        if ($installed_counter = count($updates_installed)) {
            $result = $this->getSqlConnectorApp()->getTranslator()->translate('SCHEMA_INSTALLER.SUCCESS', array(
                '%counter%' => $installed_counter
            ), $installed_counter);
        }
        if ($failed_counter = count($updates_failed)) {
            $result_failed = $this->getSqlConnectorApp()->getTranslator()->translate('SCHEMA_INSTALLER.FAILED', array(
                '%counter%' => $failed_counter,
                '%first_failed_id%' => reset($updates_failed),
                '%error_text%' => $error_text
            ), $failed_counter);
        }
        
        if ($result && $result_failed) {
            $result = $result . '. ' . $result_failed;
        } elseif ($result_failed) {
            $result = $result_failed;
        }
        $result = $result ? " \n" . $result . '. ' : $result;
        
        return $result;
    }

    /**
     *
     * @param string $id            
     * @return $this
     */
    protected function setLastModelSourceUpdateId($id)
    {
        $filename = $this->getWorkbench()->filemanager()->getPathToConfigFolder() . DIRECTORY_SEPARATOR . $this->getApp()->getConfigFileName();
        
        // Load the installation specific config file
        $config = ConfigurationFactory::create($this->getWorkbench());
        if (file_exists($filename)) {
            $config->loadConfigFile($filename);
        }
        // Overwrite the option
        $config->setOption('LAST_PERFORMED_MODEL_SOURCE_UPDATE_ID', $id);
        // Save the file or create one if there was no installation specific config before
        file_put_contents($filename, $config->exportUxonObject()->toJson(true));
        
        return $this;
    }

    /**
     *
     * @return SqlDataConnectorApp
     */
    protected function getSqlConnectorApp()
    {
        return $this->getWorkbench()->getApp('exface.SqlDataConnector');
    }
}
?>