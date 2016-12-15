<?php namespace exface\SqlDataConnector;

use exface\Core\CommonLogic\AbstractDataQuery;

class SqlDataQuery extends AbstractDataQuery {
	
	private $sql = '';
	private $result_array = array();
	private $result_resource = null;
	private $affected_row_counter = 0;
	private $last_insert_id = null;
	private $last_insert_ids = array();
	
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
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\Core\CommonLogic\AbstractDataQuery::import_string()
	 */
	public function import_string($string){
		$this->set_sql($string);
	}
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\Core\CommonLogic\AbstractDataQuery::export_uxon_object()
	 */
	public function export_uxon_object(){
		$uxon = parent::export_uxon_object();
		$uxon->set_property('sql', $this->get_sql());
		return $uxon;
	}
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\Core\Interfaces\DataSources\DataQueryInterface::count_affected_rows()
	 */
	public function count_affected_rows(){
		return $this->affected_row_counter;
	}
	
	/**
	 * 
	 * @param integer $integer
	 */
	public function set_affected_row_counter($integer){
		$this->affected_row_counter = intval($integer);
		return $this;
	}
	
	public function get_last_insert_id() {
		return $this->last_insert_id;
	}
	
	public function set_last_insert_id($value) {
		$this->last_insert_id = $value > 0 ? $value : null;
		return $this;
	}  
	
}
