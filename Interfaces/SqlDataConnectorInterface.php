<?php namespace exface\SqlDataConnector\Interfaces;

use exface\Core\Interfaces\DataSources\DataConnectionInterface;
use exface\SqlDataConnector\SqlDataQuery;

interface SqlDataConnectorInterface extends DataConnectionInterface {
	
	/**
	 * Runs any sql returning a data query instance
	 * @param string $string
	 * @return SqlDataQuery
	 */
	public function run_sql($string);
}

?>