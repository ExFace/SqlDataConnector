<?php namespace exface\SqlDataConnector\DataConnectors;

use exface\Core\CommonLogic\AbstractDataConnector;
use exface\SqlDataConnector\Interfaces\SqlDataConnectorInterface;
use exface\SqlDataConnector\SqlDataQuery;

/** 
 * Datbase API object of Microsoft SQL Server
 * Written by Andrej Kabachnik, 2015
 *
 */
abstract class AbstractSqlConnector extends AbstractDataConnector implements SqlDataConnectorInterface {

	private $current_connection;
	private $connected;
	private $autocommit = true;
	private $transaction_started = false;
	
	/**
	 * @return boolean
	 */
	protected function get_autocommit() {
		return $this->autocommit;
	}
	
	/**
	 * 
	 * @param boolean $value
	 */
	protected function set_autocommit($value) {
		if (is_numeric($value)){
			$value = $value == 0 ? false : true;
		} elseif (strcasecmp($value, 'false') == 0){
			$value = false;
		} else {
			$value = true;
		}
		$this->autocommit = $value;
		return $this;
	}
	
	public function get_current_connection() {
		return $this->current_connection;
	}
	
	public function set_current_connection(&$value) {
		$this->current_connection = $value;
		$this->set_connected(true);
		return $this;
	}
	
	public function is_connected() {
		return $this->connected;
	}
	
	public function set_connected($value) {
		$this->connected = $value;
		return $this;
	}
	
	public function transaction_is_started() {
		return $this->transaction_started;
	}
	
	public function set_transaction_started($value) {
		$this->transaction_started = $value;
		return $this;
	} 
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\SqlDataConnector\Interfaces\SqlDataConnectorInterface::run_sql()
	 */
	public function run_sql($string){
		$query = new SqlDataQuery();
		$query->set_sql($string);
		return $this->query($query);
	}
	
	/**
	 * 
	 * @param resource $result
	 * @return array
	 */
	abstract public function make_array($result);
}
?>