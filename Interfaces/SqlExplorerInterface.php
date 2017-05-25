<?php
namespace exface\SqlDataConnector\Interfaces;

use exface\Core\CommonLogic\Model\Object;

interface SqlExplorerInterface
{

    public function __construct(SqlDataConnectorInterface $data_connector);

    /**
     *
     * @param Object $meta_object            
     * @param string $table_name            
     *
     */
    public function getAttributePropertiesFromTable(Object $meta_object, $table_name);

    /**
     *
     * @return SqlDataConnectorInterface
     */
    public function getDataConnection();
}

?>