<?php namespace exface\SqlDataConnector\DataConnectors;

use exface\Core\CommonLogic\AbstractDataConnector;
use exface\Core\Exceptions\DataSources\DataConnectionFailedError;
use exface\Core\Exceptions\DataSources\DataConnectionCommitFailedError;
use exface\Core\Exceptions\DataSources\DataConnectionRollbackFailedError;
use exface\SqlDataConnector\SqlDataQuery;
use exface\Core\Exceptions\DataSources\DataQueryFailedError;

/** 
 * Generic connector for ODBC SQL data sources 
 * 
 * @author Andrej Kabachnik
 */
class OdbcSqlConnector extends AbstractSqlConnector {
	
	private $dsn = null;
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\Core\CommonLogic\AbstractDataConnector::perform_connect()
	 */
	protected function perform_connect() {
		$conn = null;
		$conn_exception = null;
		
		// Connect
		try {
			$conn = @odbc_connect($this->get_dsn(), $this->get_user(), $this->get_password(), SQL_CUR_USE_ODBC);
		} catch (\Exception $e) {
			$conn = 0;
			$conn_exception = $e;
		}
		
		if (!$conn){
			throw new DataConnectionFailedError($this, 'Failed to create the database connection for "' . $this->get_alias_with_namespace() . '"!', '6T2TBVR', $conn_exception);
		}
			
		// Apply autocommit option
		if ($this->get_autocommit()){
			odbc_autocommit($conn, 1);
		} else {
			odbc_autocommit($conn, 0);
		}
		
		$this->set_current_connection($conn);
	}
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\Core\CommonLogic\AbstractDataConnector::perform_disconnect()
	 */
	protected function perform_disconnect() {
		try {
			@odbc_close($this->get_current_connection());
		} catch (\Throwable $e){
			// ignore errors on close
		}
	}
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\Core\CommonLogic\AbstractDataConnector::perform_query()
	 * @param SqlDataQuery $query
	 */
	protected function perform_query_sql(SqlDataQuery $query) {	
		try {
			$result = odbc_exec($this->get_current_connection(), $query->get_sql());
			$query->set_result_resource($result);
		} catch (\Exception $e){
			throw new DataQueryFailedError($query, "ODBC SQL query failed! " . $e->getMessage(), '6T2T2UI', $e);
			
		}
		return $query;
	}

	protected function get_last_error() {
		return odbc_error($this->get_current_connection()) . ' (' . odbc_errormsg($this->get_current_connection() . ')');
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
		while ($row = odbc_fetch_array($rs)) {
			$array[] = $row;
		}
		return $array;
	}
	
	public function get_insert_id(SqlDataQuery $query){
		try {
			// TODO
		} catch (\Exception $e){
			throw new DataQueryFailedError($query, "Cannot get insert_id for SQL query: " . $e->getMessage(), '6T2TCAJ', $e);
		}
	}
	
	public function get_affected_rows_count(SqlDataQuery $query){
		try {
			return odbc_num_rows($query->get_result_resource());
		} catch (\Exception $e){
			throw new DataQueryFailedError($query, "Cannot count affected rows in ODBC SQL query: " . $e->getMessage(), '6T2TCL6', $e);
		}
	}
	
	public function transaction_start(){
		if (!$this->transaction_is_started()){
			$this->set_transaction_started(true);
		}
		return $this;
	}
	
	public function transaction_commit(){
		// Do nothing if the autocommit option is set for this connection
		if ($this->get_autocommit()){
			return $this;
		}
		
		try {
			return odbc_commit($this->get_current_connection());
		} catch (\Exception $e){
			throw new DataConnectionCommitFailedError($this, "Commit failed: " . $e->getMessage(), '6T2T2O9', $e);
		}
		return $this;
	}
	
	public function transaction_rollback(){
		// Throw error if trying to rollback a transaction with autocommit enabled
		if ($this->get_autocommit()){
			throw new DataConnectionRollbackFailedError($this, 'Cannot rollback transaction in "' . $this->get_alias_with_namespace() . '": The autocommit options is set to TRUE for this connection!');
		}
		
		try {
			return odbc_rollback($this->get_current_connection());
		} catch (\Exception $e){
			throw new DataConnectionRollbackFailedError($this, "Rollback failed: " . $e->getMessage(), '6T2T2S1', $e);
		}
		return $this;
	}
	
	public function free_result(SqlDataQuery $query){
		odbc_free_result($query->get_result_resource());
	}
	
	/**
	 *
	 * {@inheritDoc}
	 * @see \exface\SqlDataConnector\DataConnectors\AbstractSqlConnector::export_uxon_object()
	 */
	public function export_uxon_object(){
		$uxon = parent::export_uxon_object();
		$uxon->set_property('dbase', $this->get_dbase());
		$uxon->set_property('use_persistant_connection', $this->get_use_persistant_connection());
		return $uxon;
	}	
	
	public function get_dsn() {
		return $this->dsn;
	}
	
	/**
	 * Sets the DSN to be used in the ODBC connection
	 *
	 * @uxon-property dsn
	 * @uxon-type string
	 *
	 * @param string $value
	 * @return OdbcSqlConnector
	 */
	public function set_dsn($value) {
		$this->dsn = $value;
		return $this;
	}  
	  
}
?>