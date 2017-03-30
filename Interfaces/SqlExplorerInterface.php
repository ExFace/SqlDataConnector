<?php namespace exface\SqlDataConnector\Interfaces;

use exface\Core\CommonLogic\Model\Object;

interface SqlExplorerInterface {
	
	public function __construct(SqlDataConnectorInterface $data_connector);
	
	/**
	 * 
	 * @param Object $meta_object
	 * @param string $table_name
	 * 
	 */
	public function get_attribute_properties_from_table(Object $meta_object, $table_name);
	
	/**
	 * @return SqlDataConnectorInterface
	 */
	public function get_data_connection();
}

?>