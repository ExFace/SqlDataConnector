<?php namespace exface\SqlDataConnector\DataConnectors;

use exface\Core\CommonLogic\AbstractDataConnector;
use exface\Core\Exceptions\DataConnectionError;
use exface\Core\Interfaces\DataSources\DataQueryInterface;
use exface\SqlDataConnector\SqlDataQuery;
use exface\Core\Exceptions\DataSources\DataQueryFailedError;

/** 
 * Data source connector for MySQL databases 
 * @author Andrej Kabachnik
 */
class MySQL extends AbstractSqlConnector {
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\Core\CommonLogic\AbstractDataConnector::perform_connect()
	 */
	protected function perform_connect($host = '', $dbase = '', $uid = '', $pwd = '', $persist = 0) {
		$uid = $uid ? $uid : $this->get_config_array()['user'];
		$pwd = $pwd ? $pwd : $this->get_config_array()['password'];
		$host = $host ? $host : $this->get_config_array()['host'];
		$dbase = $dbase ? $dbase : $this->get_config_array()['dbase'];
		$charset = $this->get_config_array()['charset'];
		$connection_method = $this->get_config_array()['connection_method'];
		if ($this->get_config_value('autocommit')){
			$this->set_autocommit($this->get_config_value('autocommit'));
		}
		$safe_count = 0;
		$conn = null;
		while(!$conn && $safe_count<3)
		{
			 if($persist!=0) {
			 	$conn = mysqli_pconnect($host, $uid, $pwd, $dbase);
			 } else {
			 	$conn = mysqli_connect($host, $uid, $pwd, $dbase);
			 }
			 
			 if(!$conn)
			 {
				sleep(1);
				$safe_count++;
			 }
		}
		if (!$conn) {
			throw new DataConnectionError('Failed to create the database connection for "' . $this->get_alias_with_namespace() . '"!');
		} else {
			mysqli_query($conn, "{$connection_method} {$charset}");
				if (function_exists('mysqli_set_charset')) {
					 mysqli_set_charset($conn, $this->get_config_array()['charset']);
				} else {
					 mysqli_query($conn, "SET NAMES {$this->get_config_array()['charset']}");
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
		@ mysqli_close($this->get_current_connection());
	}
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\Core\CommonLogic\AbstractDataConnector::perform_query()
	 * @param SqlDataQuery $query
	 */
	protected function perform_query(DataQueryInterface $query) {
		if (!($query instanceof SqlDataQuery)){
			throw new DataConnectionError('The MySQL data connector expects an SqlDataQuery as input: "' . get_class($query) . '" given instead!');
		}
		
		if (is_null($this->get_current_connection()) || !is_resource($this->get_current_connection())) {
			$this->connect();
		}
		
		if (!$result = mysqli_query($this->get_current_connection(), $query->get_sql())) {
			throw new DataQueryFailedError($query, "SQL query failed: " . $this->get_last_error());
			return $query;
		} else {
			$query->set_result_array($this->make_array($result));
			$query->set_affected_row_counter(mysqli_affected_rows($this->get_current_connection()));
			$query->set_last_insert_id(mysqli_insert_id($this->get_current_connection()));
			return $query;
		}
	}

	protected function get_last_error() {
		return mysqli_error($this->get_current_connection()) . ' (Error ' . mysqli_errno($this->get_current_connection() . ')');
	}
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\SqlDataConnector\DataConnectors\AbstractSqlConnector::make_array()
	 */
	protected function make_array($rs){
		if(!($rs instanceof \mysqli_result)) return array();
		$array = array();
		while ($row = mysqli_fetch_assoc($rs)) {
			$array[] = $row;
		}
		return $array;
	}
	
	public function transaction_start(){
		// TODO after migrating to mysqli
	}
	
	public function transaction_commit(){
		// TODO after migrating to mysqli
	}
	
	public function transaction_rollback(){
		// TODO after migrating to mysqli
	}
}
?>