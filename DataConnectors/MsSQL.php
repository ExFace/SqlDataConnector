<?php namespace exface\SqlDataConnector\DataConnectors;

use exface\Core\CommonLogic\AbstractDataConnector;
use exface\Core\Exceptions\DataSources\DataConnectionFailedError;
use exface\Core\Exceptions\DataSources\DataConnectionTransactionStartError;
use exface\Core\Exceptions\DataSources\DataConnectionCommitFailedError;
use exface\Core\Exceptions\DataSources\DataConnectionRollbackFailedError;
use exface\SqlDataConnector\SqlDataQuery;
use exface\Core\Exceptions\DataSources\DataQueryFailedError;

/** 
 * Datbase API object of Microsoft SQL Server
 * @author Andrej Kabachnik
 *
 */

class MsSQL extends AbstractSqlConnector {
	
	private $Database = null;
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\Core\CommonLogic\AbstractDataConnector::perform_connect()
	 */
	protected function perform_connect() {
		$connectInfo = array();		
		$connectInfo["Database"] = $this->get_Database();
		$connectInfo["CharacterSet"] = $this->get_CharacterSet();
		if ($this->get_UID()) $connectInfo["UID"] = $this->get_UID();
		if ($this->get_PWD()) $connectInfo["PWD"] = $this->get_PWD();
		
		if (!$conn = sqlsrv_connect($this->get_serverName() . ($this->get_port() ? ', ' . $this->get_port() : ''), $connectInfo)) {
			throw new DataConnectionFailedError($this, "Failed to create the database connection! " . $this->get_last_error());
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
			throw new DataQueryFailedError($query, "SQL query failed! " . $this->get_last_error(), '6T2T2UI');
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
			throw new DataConnectionTransactionStartError($this, 'Cannot start transaction in "' . $this->get_alias_with_namespace() . '": ' . $this->get_last_error(), '6T2T2JM');
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
			throw new DataConnectionCommitFailedError($this, 'Cannot commit transaction in "' . $this->get_alias_with_namespace() . '": ' . $this->get_last_error(), '6T2T2O9');
		} else {
			$this->set_transaction_started(false);
		}
		return $this;
	}
	
	public function transaction_rollback(){
		// Throw error if trying to rollback a transaction with autocommit enabled
		if ($this->get_autocommit()){
			throw new DataConnectionRollbackFailedError($this, 'Cannot rollback transaction in "' . $this->get_alias_with_namespace() . '": The autocommit options is set to TRUE for this connection!');
		}
		
		if (!sqlsrv_begin_transaction($this->get_current_connection())){
			throw new DataConnectionRollbackFailedError($this, 'Cannot rollback transaction in "' . $this->get_alias_with_namespace() . '": ' . $this->get_last_error(), '6T2T2S1');
		} else {
			$this->set_transaction_started(false);
		}
		return $this;
	}	
	
	public function free_result(SqlDataQuery $query){
		sqlsrv_free_stmt($query->get_result_resource());
	}
	
	
	public function get_UID() {
		return $this->get_user();
	}
	
	/**
	 * Sets the user id for the connection (same as "user")
	 *
	 * @uxon-property UID
	 * @uxon-type string
	 *
	 * @see set_user()
	 * @param string $value
	 * @return MsSQL
	 */
	public function set_UID($value) {
		return $this->set_user($value);
	}
	
	public function get_PWD() {
		return $this->get_password();
	}
	
	/**
	 * Sets the password for the connection (same as "password")
	 *
	 * @uxon-property PWD
	 * @uxon-type string
	 *
	 * @see set_password()
	 * @param string $value
	 * @return MsSQL
	 */
	public function set_PWD($value) {
		return $this->set_password($value);
	}
	
	public function get_serverName() {
		return $this->get_host();
	}
	
	/**
	 * Sets the server name for the connection (same as "host")
	 *
	 * @uxon-property serverName
	 * @uxon-type string
	 * 
	 * @see set_host()
	 * @param string $value
	 * @return MsSQL
	 */
	public function set_serverName($value) {
		return $this->set_host($value);
	}
	
	public function get_Database() {
		return $this->DataBase;
	}
	
	public function set_Database($value) {
		$this->DataBase = $value;
		return $this;
	}
	
	public function get_CharacterSet() {
		return $this->get_character_set();
	}
	
	public function set_CharacterSet($value) {
		return $this->set_character_set($value);
	}
	
	/**
	 *
	 * {@inheritDoc}
	 * @see \exface\SqlDataConnector\DataConnectors\AbstractSqlConnector::export_uxon_object()
	 */
	public function export_uxon_object(){
		$uxon = parent::export_uxon_object();
		$uxon->set_property('Database', $this->get_Database());
		return $uxon;
	}	
	      
}
?>