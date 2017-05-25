<?php
namespace exface\SqlDataConnector;

use exface\Core\CommonLogic\Model\Object;
use exface\Core\CommonLogic\NameResolver;
use exface\Core\Exceptions\UnexpectedValueException;
use exface\Core\Interfaces\InstallerInterface;
use exface\Core\CommonLogic\AbstractApp;
use exface\SqlDataConnector\SqlExplorer\OracleSQLExplorer;
use exface\SqlDataConnector\SqlExplorer\MySQLExplorer;
use exface\SqlDataConnector\SqlExplorer\MSSQLExplorer;
use exface\SqlDataConnector\SqlExplorer\AbstractSQLExplorer;

class SqlDataConnectorApp extends AbstractApp
{

    const FOLDER_WITH_MODEL_SOURCE_SQL = 'ModelSource';

    const FOLDER_WITH_MODEL_SOURCE_SQL_UPDATES = 'updates';

    public function getInstaller(InstallerInterface $injected_installer = null)
    {
        $installer = parent::getInstaller($injected_installer);
        
        // First of all update the meta model source schema if the model is stored in SQL (= if the connector
        // used by the model loader is an SQL connector). This needs to be done before any other installers are run -
        // in particular before the model installer, because the model installer will attempt to write to the new
        // SQL schema already!
        // Although the core app will update it's model schema by itself, this step is also needed here, because the
        // schema updates or fixes may come asynchronously to core app updates. If the core app already udated the
        // schema, the installer won't do anything, so we will not run into conflicts.
        if ($this->containsClass($this->getWorkbench()->model()->getModelLoader())) {
            $installer->addInstaller($this->getWorkbench()->model()->getModelLoader()->getInstaller(), true);
        }
        return $installer;
    }
}
?>