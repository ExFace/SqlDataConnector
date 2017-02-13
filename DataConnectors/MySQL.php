<?php namespace exface\SqlDataConnector\DataConnectors;

use exface\Core\CommonLogic\AbstractDataConnector;
use exface\Core\Exceptions\DataSources\DataConnectionFailedError;
use exface\Core\Exceptions\DataSources\DataConnectionTransactionStartError;
use exface\Core\Exceptions\DataSources\DataConnectionCommitFailedError;
use exface\Core\Exceptions\DataSources\DataConnectionRollbackFailedError;
use exface\SqlDataConnector\SqlDataQuery;
use exface\Core\Exceptions\DataSources\DataQueryFailedError;

/** 
 * Data source connector for MySQL databases 
 * @author Andrej Kabachnik
 */
class MySQL extends AbstractSqlConnector {
	
	private $dbase = null;
	private $connection_method = null;
	private $use_persistant_connection = false;
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\Core\CommonLogic\AbstractDataConnector::perform_connect()
	 */
	protected function perform_connect() {
		$safe_count = 0;
		$conn = null;
		
		// Make mysqli produce exceptions instead of errors
		mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
		
		while(!$conn && $safe_count<3) {
			try {
				if($this->get_use_persistant_connection()) {
			 		$conn = mysqli_pconnect($this->get_host(), $this->get_user(), $this->get_password(), $this->get_dbase());
				} else {
			 		$conn = mysqli_connect($this->get_host(), $this->get_user(), $this->get_password(), $this->get_dbase());
				}
			} catch (\mysqli_sql_exception $e) {
				// Do nothing, try again later
			}
			if(!$conn) {
				sleep(1);
				$safe_count++;
			}
		}
		if (!$conn) {
			throw new DataConnectionFailedError($this, 'Failed to create the database connection for "' . $this->get_alias_with_namespace() . '"!', '6T2TBVR', $e);
		} else {
			// Apply autocommit option
			if ($this->get_autocommit()){
				mysqli_autocommit($conn, true);
			} else {
				mysqli_autocommit($conn, false);
			}
			
			// Set the character set
			// mysqli_query($conn, "{$this->get_connection_method()} {$this->get_charset()}");			
			if (function_exists('mysqli_set_charset')) {
				 mysqli_set_charset($conn, $this->get_charset());
			} else {
				 mysqli_query($conn, "SET NAMES {$this->get_charset()}");
			}
			
			$this->set_current_connection($conn);
		}
	}
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\Core\CommonLogic\AbstractDataConnector::perform_disconnect()
	 */
	protected function perform_disconnect() {
		try {
			mysqli_close($this->get_current_connection());
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
			$result = mysqli_query($this->get_current_connection(), $query->get_sql());
			$query->set_result_resource($result);
		} catch (\mysqli_sql_exception $e){
			throw new DataQueryFailedError($query, "SQL query failed! " . $e->getMessage(), '6T2T2UI', $e);
			
		}
		return $query;
	}

	protected function get_last_error() {
		return mysqli_error($this->get_current_connection()) . ' (Error ' . mysqli_errno($this->get_current_connection() . ')');
	}
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\SqlDataConnector\DataConnectors\AbstractSqlConnector::make_array()
	 */
	public function make_array(SqlDataQuery $query){
		$rs = $query->get_result_resource();
		if(!($rs instanceof \mysqli_result)) return array();
		$array = array();
		while ($row = mysqli_fetch_assoc($rs)) {
			$array[] = $row;
		}
		return $array;
	}
	
	public function get_insert_id(SqlDataQuery $query){
		try {
			return mysqli_insert_id($this->get_current_connection());
		} catch (\mysqli_sql_exception $e){
			throw new DataQueryFailedError($query, "Cannot get insert_id for SQL query: " . $e->getMessage(), '6T2TCAJ', $e);
		}
	}
	
	public function get_affected_rows_count(SqlDataQuery $query){
		try {
			return mysqli_affected_rows($this->get_current_connection());
		} catch (\mysqli_sql_exception $e){
			throw new DataQueryFailedError($query, "Cannot count affected rows in SQL query: " . $e->getMessage(), '6T2TCL6', $e);
		}
	}
	
	public function transaction_start(){
		if (!$this->transaction_is_started()){
			try {
				mysqli_begin_transaction ($this->get_current_connection());
				$this->set_transaction_started(true);
			} catch (\mysqli_sql_exception $e){
				throw new DataConnectionTransactionStartError($this, "Cannot start transaction: " . $e->getMessage(), '6T2T2JM', $e);
			}
		}
		return $this;
	}
	
	public function transaction_commit(){
		// Do nothing if the autocommit option is set for this connection
		if ($this->get_autocommit()){
			return $this;
		}
		
		try {
			return mysqli_commit($this->get_current_connection());
		} catch (\mysqli_sql_exception $e){
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
			return mysqli_rollback($this->get_current_connection());
		} catch (\mysqli_sql_exception $e){
			throw new DataConnectionRollbackFailedError($this, "Rollback failed: " . $e->getMessage(), '6T2T2S1', $e);
		}
		return $this;
	}
	
	public function free_result(SqlDataQuery $query){
		mysqli_free_result($query->get_result_resource());
	}
	
	public function get_dbase() {
		return $this->dbase;
	}
	
	/**
	 * Sets the database name to be used in this connection
	 *
	 * @uxon-property dbase
	 * @uxon-type string
	 *
	 * @param string $value
	 * @return MySQL
	 */
	public function set_dbase($value) {
		$this->dbase = $value;
		return $this;
	}
	
	public function get_connection_method() {
		return $this->connection_method;
	}

	/**
	 * Sets the connection method to be used in this connection
	 *
	 * @uxon-property connection_method
	 * @uxon-type string
	 *
	 * @param string $value
	 * @return MySQL
	 */
	public function set_connection_method($value) {
		$this->connection_method = $value;
		return $this;
	}  
	
	public function get_charset() {
		return $this->get_character_set();
	}
	

	/**
	 * Sets the character set to be used in this connection (same as "character_set")
	 *
	 * @uxon-property charset
	 * @uxon-type string
	 *
	 * @see set_character_set()
	 * @param string $value
	 * @return MySQL
	 */
	public function set_charset($value) {
		return $this->set_character_set($value);
	}  
	
	public function get_use_persistant_connection() {
		return $this->use_persistant_connection;
	}
	
	/**
	 * Set to TRUE to use persistant connections. 
	 *
	 * @uxon-property use_persistant_connection
	 * @uxon-type boolean
	 *
	 * @see set_character_set()
	 * @param boolean $value
	 * @return MySQL
	 */
	public function set_use_persistant_connection($value) {
		$this->use_persistant_connection = $value ? true : false;
		return $this;
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
	  
}
?>