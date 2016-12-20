<?php namespace exface\SqlDataConnector;

use exface\Core\CommonLogic\AbstractDataQuery;
use exface\SqlDataConnector\Interfaces\SqlDataConnectorInterface;

class SqlDataQuery extends AbstractDataQuery {
	
	private $sql = '';
	private $result_array = null;
	private $result_resource = null;
	private $connection = null;
	
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
		if (is_null($this->result_array)){
			return $this->get_connection()->make_array($this);
		}
		return $this->result_array;
	}
	
	public function set_result_array(array $value) {
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
		return $this->get_connection()->get_affected_rows_count($this);
	}
	
	public function get_last_insert_id() {
		return $this->get_connection()->get_insert_id($this);
	} 
	
	/**
	 * 
	 * @return \exface\SqlDataConnector\Interfaces\SqlDataConnectorInterface
	 */
	public function get_connection() {
		return $this->connection;
	}
	
	public function set_connection(SqlDataConnectorInterface $value) {
		$this->connection = $value;
		return $this;
	}	
	
	public function free_result(){
		$this->get_connection()->free_result($this);
	}
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\Core\CommonLogic\AbstractDataQuery::to_string()
	 */
	public function to_string(){
		return \SqlFormatter::format($this->get_sql(), false);
	}
	
}
