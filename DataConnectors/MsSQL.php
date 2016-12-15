<?php namespace exface\SqlDataConnector\DataConnectors;

use exface\Core\CommonLogic\AbstractDataConnector;
use exface\Core\Exceptions\DataConnectionError;
use exface\SqlDataConnector\SqlDataQuery;
use exface\Core\Exceptions\DataSources\DataQueryFailedError;

/** 
 * Datbase API object of Microsoft SQL Server
 * @author Andrej Kabachnik
 *
 */

class MsSQL extends AbstractSqlConnector {
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\Core\CommonLogic\AbstractDataConnector::perform_connect()
	 */
	protected function perform_connect($host = '', $port = '', $database = '', $uid = '', $pwd = '', $charset = null) {
		$connectInfo = array();
		$uid = $uid ? $uid : $this->get_config_array()['UID'];
		$pwd = $pwd ? $pwd : $this->get_config_array()['PWD'];
		$host = $host ? $host : $this->get_config_array()['serverName'];
		$port = $port ? $port : $this->get_config_array()['port'];
		if ($this->get_config_value('autocommit')){
			$this->set_autocommit($this->get_config_value('autocommit'));
		}
		
		$connectInfo["Database"] = $database ? $database : $this->get_config_array()['Database'];
		$connectInfo["CharacterSet"] = $charset ? $charset : $this->get_config_array()['CharacterSet'];
		if ($uid) $connectInfo["UID"] = $uid;
		if ($pwd) $connectInfo["PWD"] = $pwd;
		if (!$conn = sqlsrv_connect($host . ($port ? ', ' . $port : ''), $connectInfo)) {
			throw new DataConnectionError("Failed to create the database connection! " . $this->get_last_error($this->get_current_connection()));
		} else {
			$this->set_current_connection($conn);
		}
	}
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\Core\CommonLogic\AbstractDataConnector::perform_disconnect()
	 */
	protected function perform_disconnect() {
		@ sqlsrv_close($this->get_current_connection());
	}
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\Core\CommonLogic\AbstractDataConnector::perform_query()
	 * 
	 * @param SqlDataQuery $query
	 */
	protected function perform_query_sql(SqlDataQuery $query) {		
		if (!$result = sqlsrv_query($this->get_current_connection(), $query->get_sql())) {
			throw new DataQueryFailedError($query, "SQL query failed: " . $this->get_last_error());
		} else {
			$query->set_result_resource($result);
			return $query;
		}
	}

	function get_insert_id(SqlDataQuery $query) {
		$id = ""; 
		$rs = sqlsrv_query("SELECT @@identity AS id"); 
		if ($row = mssql_fetch_row($rs)) { 
			$id = trim($row[0]); 
		} 
		mssql_free_result($rs); 
		return $id; 
	}
	
	function get_affected_rows_count(SqlDataQuery $query) {
		return sqlsrv_rows_affected($this->get_current_connection());
	}

	protected function get_last_error() {
		$errors = $this->get_errors();
		return $errors[0]['message'];
	}
	
	protected function get_errors() {
			return sqlsrv_errors();
	}

	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\SqlDataConnector\DataConnectors\AbstractSqlConnector::make_array()
	 */
	public function make_array(SqlDataQuery $query){
		$rs = $query->get_result_resource();
		if(!$rs) return array();
		$array = array();
		while ($row = sqlsrv_fetch_array($rs, SQLSRV_FETCH_ASSOC)) {
			$array[] = $row;
		}
		return $array;
	}  
	
	public function transaction_start(){
		// Do nothing if the autocommit option is set for this connection
		if ($this->get_autocommit()){
			return $this;
		}
		
		if (!sqlsrv_begin_transaction($this->get_current_connection())){
			throw new DataConnectionError('Cannot start transaction in "' . $this->get_alias_with_namespace() . '": ' . $this->get_last_error());
		} else {
			$this->set_transaction_started(true);
		}
		return $this;
	}
	
	public function transaction_commit(){
		// Do nothing if the autocommit option is set for this connection
		if ($this->get_autocommit()){
			return $this;
		}
		
		if (!sqlsrv_commit($this->get_current_connection())){
			throw new DataConnectionError('Cannot commit transaction in "' . $this->get_alias_with_namespace() . '": ' . $this->get_last_error());
		} else {
			$this->set_transaction_started(false);
		}
		return $this;
	}
	
	public function transaction_rollback(){
		// Do nothing if the autocommit option is set for this connection
		if ($this->get_autocommit()){
			return $this;
		}
		
		if (!sqlsrv_begin_transaction($this->get_current_connection())){
			throw new DataConnectionError('Cannot rollback transaction in "' . $this->get_alias_with_namespace() . '": ' . $this->get_last_error());
		} else {
			$this->set_transaction_started(false);
		}
		return $this;
	}	
	
	public function free_result(SqlDataQuery $query){
		sqlsrv_free_stmt($query->get_result_resource());
	}
}
?>