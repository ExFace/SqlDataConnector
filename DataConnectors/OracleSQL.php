<?php namespace exface\SqlDataConnector\DataConnectors;

use exface\Core\CommonLogic\AbstractDataConnector;
use exface\Core\Exceptions\DataConnectionError;

/**
 * Datbase API object of OracleSQL
 * @author Andrej Kabachnik
 *
 */
class OracleSQL extends AbstractSqlConnector {
	
	private $rows_affected_by_last_query = 0;
	private $insert_id_field_name = 'OID';
	private $last_insert_id = NULL;
	private $last_statement = NULL;
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\Core\CommonLogic\AbstractDataConnector::perform_connect()
	 */
	protected function perform_connect($host = '', $port = '', $sid = '', $uid = '', $pwd = '', $charset='', $persist = 0) {
		$uid = $uid ? $uid : $this->get_config_array()['user'];
		$pwd = $pwd ? $pwd : $this->get_config_array()['password'];
		$host = $host ? $host : $this->get_config_array()['host'];
		$port = $port ? $port : $this->get_config_array()['port'];
		$sid = $sid ? $sid : $this->get_config_array()['sid'];
		$charset = $charset ? $charset : $this->get_config_array()['character_set'];
		if ($this->get_config_value('autocommit')){
			$this->set_autocommit($this->get_config_value('autocommit'));
		}
		if (!$conn = oci_connect($uid, $pwd, $host.':'.$port.'/'.$sid, $charset)) {
			throw new DataConnectionError('Failed to create the database connection for "' . $this->get_alias_with_namespace() . '"!');
		} else {
			$this->set_current_connection($conn);
			// Set default date and time formats to ensure compatibility with ExFace
			$this->perform_query("ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS' NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS' NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'", array('return_raw_result' => true));
		}
	}
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\Core\CommonLogic\AbstractDataConnector::perform_disconnect()
	 */
	protected function perform_disconnect() {
		@ oci_close($this->get_current_connection());
	}
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\Core\CommonLogic\AbstractDataConnector::perform_query()
	 */
	protected function perform_query($sql, $options = null) {
		$return_raw_result = $options['return_raw_result'];
		if (is_null($this->get_current_connection()) || !is_resource($this->get_current_connection())) {
			$this->connect();
		}
		
		// If the statement is an insert, add a clause to save the autogenerated insert id
		if (substr($sql, 0, 6) == 'INSERT' && $this->insert_id_field_name){
			$sql .= ' returning ' . $this->insert_id_field_name . ' into :id';
		}
		
		if (!$result = oci_parse($this->get_current_connection(), $sql)) {
			throw new DataConnectionError("Pasrsing of a query to the database failed!", $sql);
		} else {
			$this->last_statement = $result;
			// If the statement is an insert, bind the autogenerated id to the variabel $id
			if (substr($sql, 0, 6) == 'INSERT'){
				OCIBindByName($result,":ID",$id,32);
			}
			
			if ($this->get_autocommit()){
				$ex = @oci_execute($result);
			} else {
				$ex = @oci_execute($result, OCI_NO_AUTO_COMMIT);
			}
			$this->rows_affected_by_last_query = oci_num_rows($result);
			if (!$ex) {
				throw new DataConnectionError("Execution of a query to the database failed - " . $this->get_last_error());
			} else {
				
				if ($return_raw_result){
					return $result;
				} else {
					$array = $this->make_array($result);
					oci_free_statement($result);
					return $array;
				}
			}
		}
	}
	
	public function set_insert_id_field_name($value){
		$this->insert_id_field_name = $value;
	}

	function get_insert_id() {
		return $this->last_insert_id;
	}

	function get_affected_rows_count() {
		return $this->rows_affected_by_last_query;
	}

	function get_last_error() {
		$error = oci_error($this->last_statement);
		return $error['message'];
	}

	/**
	* @name:  make_array
	* @desc:  turns a recordset into a multidimensional array
	* @return: an array of row arrays from recordset, or empty array
	*			 if the recordset was empty, returns false if no recordset
	*			 was passed
	* @param: $rs Recordset to be packaged into an array
	*/
	function make_array($rs=''){
		if(!$rs) return array();
		$rsArray = array();
		while ($row = @ oci_fetch_assoc($rs)) {
			$rsArray[] = $row;
		}
		return $rsArray;
	}
	
	/**
	 * {@inheritDoc}
	 * The OCI8 connector will start a transaction automatically with the first writing excecute()
	 * @see \exface\Core\CommonLogic\AbstractDataConnector::transaction_start()
	 */
	public function transaction_start(){
		return $this;
	}
	
	public function transaction_commit(){
		// Do nothing if the autocommit option is set for this connection
		if ($this->get_autocommit()){
			return $this;
		}
		
		if (!oci_commit($this->get_current_connection())){
			throw new DataConnectionError('Cannot commit transaction in "' . $this->get_alias_with_namespace() . '": ' . $this->get_last_error());
		}
		return $this;
	}
	
	public function transaction_rollback(){
		// Do nothing if the autocommit option is set for this connection
		if ($this->get_autocommit()){
			return $this;
		}
		
		if (!oci_rollback($this->get_current_connection())){
			throw new DataConnectionError('Cannot rollback transaction in "' . $this->get_alias_with_namespace() . '": ' . $this->get_last_error());
		}
		return $this;
	}
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\Core\CommonLogic\AbstractDataConnector::transaction_is_started()
	 */
	public function transaction_is_started(){
		return false;
	}	  
}
?>