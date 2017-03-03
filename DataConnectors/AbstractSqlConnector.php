<?php namespace exface\SqlDataConnector\DataConnectors;

use exface\Core\CommonLogic\AbstractDataConnector;
use exface\SqlDataConnector\Interfaces\SqlDataConnectorInterface;
use exface\SqlDataConnector\SqlDataQuery;
use exface\Core\Interfaces\DataSources\DataQueryInterface;

/**
 * 
 * @author Andrej Kabachnik
 *
 */
abstract class AbstractSqlConnector extends AbstractDataConnector implements SqlDataConnectorInterface {

	private $current_connection;
	private $connected;
	private $autocommit = false;
	private $transaction_started = false;
	private $user = null;
	private $password = null;
	private $host = null;
	private $port = null;
	private $character_set = null;
	
	/**
	 * @return boolean
	 */
	public function get_autocommit() {
		return $this->autocommit;
	}
	
	/**
	 * 
	 * @param boolean $value
	 */
	public function set_autocommit($value) {
		$this->autocommit = filter_var($value, FILTER_VALIDATE_BOOLEAN);
		return $this;
	}
	
	final protected function perform_query(DataQueryInterface $query){
		if (is_null($this->get_current_connection())) {
			$this->connect();
		}
		$query->set_connection($this);
		return $this->perform_query_sql($query);
	}
	
	abstract protected function perform_query_sql(SqlDataQuery $query);
	
	public function get_current_connection() {
		return $this->current_connection;
	}
	
	public function set_current_connection($value) {
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
		$this->transaction_started = filter_var($value, FILTER_VALIDATE_BOOLEAN);
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
	
	public function get_user() {
		return $this->user;
	}

	/**
	 * Sets the user name to be used in this connection
	 *
	 * @uxon-property user
	 * @uxon-type string
	 *
	 * @param string $value
	 * @return AbstractSqlConnector
	 */
	public function set_user($value) {
		$this->user = $value;
		return $this;
	}
	
	public function get_password() {
		return $this->password;
	}

	/**
	 * Sets the password to be used in this connection
	 *
	 * @uxon-property password
	 * @uxon-type string
	 *
	 * @param string $value
	 * @return AbstractSqlConnector
	 */
	public function set_password($value) {
		$this->password = $value;
		return $this;
	}
	
	public function get_host() {
		return $this->host;
	}

	/**
	 * Sets the host name or IP address to be used in this connection
	 *
	 * @uxon-property host
	 * @uxon-type string
	 *
	 * @param string $value
	 * @return AbstractSqlConnector
	 */
	public function set_host($value) {
		$this->host = $value;
		return $this;
	}
	
	public function get_port() {
		return $this->port;
	}

	/**
	 * Sets the port to be used in this connection
	 *
	 * @uxon-property port
	 * @uxon-type number
	 *
	 * @param integer $value
	 * @return AbstractSqlConnector
	 */
	public function set_port($value) {
		$this->port = $value;
		return $this;
	}
	
	public function get_character_set() {
		return $this->character_set;
	}

	/**
	 * Sets the character set to be used in this connection
	 *
	 * @uxon-property character_set
	 * @uxon-type string
	 *
	 * @param string $value
	 * @return AbstractSqlConnector
	 */
	public function set_character_set($value) {
		$this->character_set = $value;
		return $this;
	}
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\Core\CommonLogic\AbstractDataConnector::export_uxon_object()
	 */
	public function export_uxon_object(){
		$uxon = parent::export_uxon_object();
		$uxon->set_property('user', $this->get_user());
		$uxon->set_property('password', $this->get_password());
		$uxon->set_property('host', $this->get_host());
		$uxon->set_property('port', $this->get_port());
		$uxon->set_property('autocommit', $this->get_autocommit());
		return $uxon;
	}

}
?>