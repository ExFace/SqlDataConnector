<?php namespace exface\SqlDataConnector;

use exface\Core\CommonLogic\AbstractDataQuery;

class SqlDataQuery extends AbstractDataQuery {
	
	private $sql = '';
	private $result_array = array();
	private $result_resource = null;
	
	/**
	 * 
	 * @return string
	 */
	public function get_sql() {
		return $this->sql;
	}
	
	/**
	 * 
	 * @param string $value
	 * @return \exface\SqlDataConnector\SqlDataQuery
	 */
	public function set_sql($value) {
		$this->sql = $value;
		return $this;
	}  
	
	/**
	 * 
	 * @return boolean
	 */
	public function get_return_raw_result() {
		return $this->return_raw_result;
	}
	
	/**
	 * 
	 * @param boolean $value
	 * @return \exface\SqlDataConnector\SqlDataQuery
	 */
	public function set_return_raw_result($value) {
		$this->return_raw_result = $value ? true : false;
		return $this;
	} 
	
	public function get_result_array() {
		return $this->result_array;
	}
	
	public function set_result_array($value) {
		$this->result_array = $value;
		return $this;
	}
	
	public function get_result_resource() {
		return $this->result_resource;
	}
	
	public function set_result_resource($value) {
		$this->result_resource = $value;
		return $this;
	}      
	
}
