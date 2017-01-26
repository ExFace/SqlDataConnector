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
	
	/**
	 *
	 * @param resource $resource
	 * @return array
	 */
	public function make_array(SqlDataQuery $query);
	
	/**
	 * 
	 * @param SqlDataQuery $query
	 * @return string
	 */
	public function get_insert_id(SqlDataQuery $query);
	
	/**
	 * 
	 * @param SqlDataQuery $query
	 * @return integer
	 */
	public function get_affected_rows_count(SqlDataQuery $query);
	
	/**
	 * 
	 * @param SqlDataQuery $query
	 * @return void
	 */
	public function free_result(SqlDataQuery $query);
}

?>