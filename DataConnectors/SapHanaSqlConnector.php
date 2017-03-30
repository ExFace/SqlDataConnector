<?php namespace exface\SqlDataConnector\DataConnectors;

use exface\SqlDataConnector\SqlExplorer\SapHanaSQLExplorer;

/** 
 * SQL connector for SAP HANA based on ODBC 
 * 
 * @author Andrej Kabachnik
 */
class SqpHanaSqlConnector extends OdbcSqlConnector {
	
	/**
	 *
	 * {@inheritDoc}
	 * @see \exface\SqlDataConnector\DataConnectors\AbstractSqlConnector::get_sql_explorer()
	 */
	public function get_sql_explorer(){
		return new SapHanaSQLExplorer($this);
	}
}
?>