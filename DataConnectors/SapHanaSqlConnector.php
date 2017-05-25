<?php
namespace exface\SqlDataConnector\DataConnectors;

use exface\SqlDataConnector\SqlExplorer\SapHanaSQLExplorer;

/**
 * SQL connector for SAP HANA based on ODBC
 *
 * @author Andrej Kabachnik
 */
class SapHanaSqlConnector extends OdbcSqlConnector
{

    /**
     *
     * {@inheritdoc}
     *
     * @see \exface\SqlDataConnector\DataConnectors\AbstractSqlConnector::getSqlExplorer()
     */
    public function getSqlExplorer()
    {
        return new SapHanaSQLExplorer($this);
    }
}
?>