<?php namespace exface\SqlDataConnector\QueryBuilders;

/**
 * SQL query builder for SAP HANA database
 * 
 * This query builder is based on the MySQL syntax, which is mostly supported by SAP HANA.
 * 
 * @author Andrej Kabachnik
 *
 */
class SapHanaSqlBuilder extends MySQL {
	
	/**
	 * SAP HANA supports custom SQL statements in the GROUP BY clause. The rest is similar to MySQL
	 *  
	 * {@inheritDoc}
	 * @see \exface\SqlDataConnector\QueryBuilders\AbstractSQL::build_sql_group_by()
	 */
	protected function build_sql_group_by(\exface\Core\CommonLogic\QueryBuilder\QueryPart $qpart, $select_from = null){
		$output = '';
		if ($this->check_for_sql_statement($qpart->get_attribute()->get_data_address())){
			if (is_null($select_from)){
				$select_from = $qpart->get_attribute()->get_relation_path()->to_string() ? $qpart->get_attribute()->get_relation_path()->to_string() : $this->get_main_object()->get_alias();
			}
			$output = str_replace('[#alias#]', $select_from, $qpart->get_attribute()->get_data_address());
		} else {
			$output = parent::build_sql_group_by($qpart, $select_from);
		}
		return $output;
	}
}
