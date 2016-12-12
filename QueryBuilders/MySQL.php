<?php
namespace exface\SqlDataConnector\QueryBuilders;

use exface\Core\Exceptions\QueryBuilderException;
use exface\Core\DataTypes\AbstractDataType;
use exface\Core\CommonLogic\AbstractDataConnector;
use exface\Core\CommonLogic\Model\RelationPath;

/**
 * A query builder for oracle SQL.
 * 
 * @author Andrej Kabachnik
 *
 */
class MySQL extends AbstractSQL {
	// CONFIG
	protected $short_alias_max_length = 64; // maximum length of SELECT AS aliases
	
	/**
	 * In MySQL the select query is pretty straight-forward: there is no need to create nested queries,
	 * since MySQL natively supports selecting pages (LIMIT). However, if aggregators (GROUP BY) are used, we still need
	 * to distinguish between core and enrichment elements in order to join enrchichment stuff after all
	 * the aggregating had been done.
	 * @see \exface\DataSources\QueryBuilders\sql_abstractSQL::build_sql_query_select()
	 */
	public function build_sql_query_select(){
		$filter_object_ids = array();
		$where = '';
		$group_by = '';
		$order_by = '';
		$selects = array();
		$select = '';
		$joins = array();
		$join = '';
		$enrichment_select = '';
		$enrichment_joins = array();
		$enrichment_join = '';
		$limit = '';
		// WHERE
		$where = $this->build_sql_where($this->get_filters());
		$joins = $this->build_sql_joins($this->get_filters());
		$filter_object_ids = $this->get_filters()->get_object_ids_safe_for_aggregation();
		$where = $where ? "\n WHERE " . $where : '';
		// GROUP BY
		$group_uid_alias = '';
		foreach ($this->get_aggregations() as $qpart){
			$group_by .= ', ' . $this->build_sql_group_by($qpart);
			if (!$group_uid_alias) {
				if ($rel_path = $qpart->get_attribute()->get_relation_path()->to_string()){
					$group_uid_alias = RelationPath::relation_path_add($rel_path, $this->get_main_object()->get_related_object($rel_path)->get_uid_alias());
				}
			}
		}
		$group_by = $group_by ? ' GROUP BY ' . substr($group_by, 2) : '';
		if ($group_uid_alias){
			//$this->add_attribute($group_uid_alias);
		}
		// SELECT
		/* @var $qpart \exface\Core\CommonLogic\QueryBuilder\QueryPartSelect */
		foreach ($this->get_attributes() as $qpart){
			// First see, if the attribute has some kind of special data type (e.g. binary)
			if ($qpart->get_attribute()->get_data_address_property('SQL_DATA_TYPE') == 'binary'){
				$this->add_binary_column($qpart->get_alias());
			}
			// If the query has a GROUP BY, we need to put the UID-Attribute in the core select as well as in the enrichment select
			// otherwise the enrichment joins won't work! Be carefull to apply this rule only to the plain UID column, not to columns 
			// using the UID with aggregate functions
			if ($group_by && $qpart->get_attribute()->get_alias() === $qpart->get_attribute()->get_object()->get_uid_alias() && !$qpart->get_aggregate_function()){
				$selects[] = $this->build_sql_select($qpart, null, null, null, 'MAX');
				$enrichment_select .= ', ' . $this->build_sql_select($qpart, 'EXFCOREQ', $this->get_short_alias($qpart->get_alias()));
			}
			// if we are aggregating, leave only attributes, that have an aggregate function,
			// and ones, that are aggregated over or can be assumed unique due to set filters
			elseif (!$group_by
					|| $qpart->get_aggregate_function()
					|| $this->get_aggregation($qpart->get_alias())){
				$selects[] = $this->build_sql_select($qpart);
				$joins = array_merge($joins, $this->build_sql_joins($qpart));
			} elseif (in_array($qpart->get_attribute()->get_object()->get_id(), $filter_object_ids) !== false){
				$rels = $qpart->get_used_relations();
				$first_rel = false;
				if (!empty($rels)){
					$first_rel = reset($rels);
					$first_rel_qpart = $this->add_attribute($first_rel->get_alias());
					// IDEA this does not support relations based on custom sql. Perhaps this needs to change
					$selects[] = $this->build_sql_select($first_rel_qpart, null, null, $first_rel_qpart->get_attribute()->get_data_address(), ($group_by ? 'MAX' : null));
				}
				$enrichment_select .= ', ' . $this->build_sql_select($qpart);
				$enrichment_joins = array_merge($enrichment_joins, $this->build_sql_joins($qpart, 'exfcoreq'));
				$joins = array_merge($joins, $this->build_sql_joins($qpart));
			}
	
		}
		$select = implode(', ', array_unique($selects));
		$enrichment_select = 'EXFCOREQ.*' . ($enrichment_select ? ', ' . substr($enrichment_select, 2) : '');
		// FROM
		$from = $this->build_sql_from();
		// JOINs
		$join = implode(' ', $joins);
		$enrichment_join = implode(' ', $enrichment_joins);
		// ORDER BY
		foreach ($this->get_sorters() as $qpart){
			$order_by .= ', ' . $this->build_sql_order_by($qpart);
		}
		$order_by = $order_by ? ' ORDER BY ' . substr($order_by, 2) : '';
		
		$distinct = $this->get_select_distinct() ? 'DISTINCT ' : '';
		
		if ($this->get_limit()){
			$limit = ' LIMIT ' . $this->get_offset() . ', ' . $this->get_limit();
		}
			
		if (($group_by && $where) || $this->get_select_distinct()){
			$query = "\n SELECT " . $distinct . $enrichment_select . " FROM (SELECT " . $select . " FROM " . $from . $join . $where . $group_by . $order_by . ") EXFCOREQ " . $enrichment_join . $order_by . $limit;
		} else {
			$query = "\n SELECT " . $distinct .  $select . " FROM " . $from . $join . $where . $group_by . $order_by . $limit;
		}
		//var_dump($query);
		return $query;
	}
	
	public function build_sql_query_totals(){
		$totals_joins = array();
		$totals_core_selects = array();
		$totals_selects = array();
		if (count($this->get_totals()) > 0){
			// determine all joins, needed to perform the totals functions
			foreach ($this->get_totals() as $qpart){
				$totals_selects[] = $this->build_sql_select($qpart, 'EXFCOREQ', $this->get_short_alias($qpart->get_alias()), null, $qpart->get_function());
				$totals_core_selects[] = $this->build_sql_select($qpart);
				$totals_joins = array_merge($totals_joins, $this->build_sql_joins($qpart));
			}
		}
		
		if ($group_by){
			$totals_core_selects[] = $this->build_sql_select($this->get_attribute($this->get_main_object()->get_uid_alias()), null, null, null, 'MAX');
		}
		
		// filters -> WHERE
		$totals_where = $this->build_sql_where($this->get_filters());
		$totals_joins = array_merge($totals_joins, $this->build_sql_joins($this->get_filters()));
		
		// GROUP BY
		foreach ($this->get_aggregations() as $qpart){
			$group_by .= ', ' . $this->build_sql_group_by($qpart);
			$totals_joins = array_merge($totals_joins, $this->build_sql_joins($qpart));
		}
		
		$totals_select = count($totals_selects) ? ', ' . implode(",\n", $totals_selects) : '';
		$totals_core_select = implode(",\n", $totals_core_selects);
		$totals_from = $this->build_sql_from();
		$totals_join = implode("\n " , $totals_joins);
		$totals_where = $totals_where ? "\n WHERE " . $totals_where : '';
		$totals_group_by = $group_by ? "\n GROUP BY " . substr($group_by, 2) : '';
		
		// This is a bit of a dirty hack to get the COUNT(*) right if there is a GROUP BY. Just enforce the use of a query with enrichment
		if ($group_by && !$totals_core_select){
			$totals_core_select = '1';
		}
		
		if ($totals_core_select){
			$totals_query = "\n SELECT COUNT(*) AS EXFCNT " . $totals_select . " FROM (SELECT " . $totals_core_select . ' FROM ' . $totals_from . $totals_join . $totals_where . $totals_group_by . ") EXFCOREQ";
		} else {
			$totals_query = "\n SELECT COUNT(*) AS EXFCNT FROM " . $totals_from . $totals_join . $totals_where . $totals_group_by;
		}
		
		return $totals_query;
	}
	
	protected function prepare_where_value($value, AbstractDataType $data_type, $sql_data_type = NULL){
		if ($data_type->is(EXF_DATA_TYPE_DATE)){
			$output = "{ts '" . $value . "'}";
		} else {
			$output = parent::prepare_where_value($value, $data_type, $sql_data_type);
		}
		return $output;
	}
	
	protected function build_sql_select_null_check($select_statement, $value_if_null){
		return 'IFNULL(' . $select_statement . ', ' . (is_numeric($value_if_null) ? $value_if_null : '"' . $value_if_null . '"' ) . ')';
	}
		
	/**
	 * @see \exface\DataSources\QueryBuilders\sql_abstractSQL
	 * @param \exface\Core\CommonLogic\QueryBuilder\QueryPart $qpart
	 * @param string $select_from
	 * @param string $select_column
	 * @param string $select_as
	 * @param string $group_function
	 * @return string
	 */
	protected function build_sql_group_function(\exface\Core\CommonLogic\QueryBuilder\QueryPart $qpart, $select_from = null, $select_column = null, $select_as = null, $group_function = null){
		$output = '';
		$group_function = !is_null($group_function) ? $group_function : $qpart->get_aggregate_function();
		$group_function = trim($group_function);
		$select = $this->build_sql_select($qpart, $select_from, $select_column, false, false);
		$args = array();
		if ($args_pos = strpos($group_function, '(')){
			$func = substr($group_function, 0, $args_pos);
			$args = explode(',', substr($group_function, ($args_pos+1), -1));
		} else {
			$func = $group_function;
		}
		
		switch ($func){
			case 'SUM': case 'AVG': case 'COUNT': case 'MAX': case 'MIN':
				$output = $func . '(' . $select . ')';
				break;
			case 'LIST_DISTINCT':
			case 'LIST':
				$output = "GROUP_CONCAT(" . ($func == 'LIST_DISTINCT' ? 'DISTINCT ' : '') . $select . " SEPARATOR " . ($args[0] ? $args[0] : "', '") . ")";
				$qpart->get_query()->add_aggregation($qpart->get_attribute()->get_alias_with_relation_path());
				break;
			case 'COUNT_DISTINCT':
				$output = "COUNT(DISTINCT " . $select . ")";
				break;
			default:
				break;
		}
		return $output;
	}
	
	/**
	 * Special DELETE builder for MySQL because MySQL does not support table aliases in the DELETE query.
	 * Thus, they must be removed from all the generated filters and other parts of the query.
	 * @see \exface\DataSources\QueryBuilders\sql_abstractSQL::delete()
	 */
	function delete(AbstractDataConnector $data_connection = null){
		// filters -> WHERE
		// Relations (joins) are not supported in delete clauses, so check for them first!
		if (count($this->get_filters()->get_used_relations()) > 0){
			throw new QueryBuilderException('Filters over attributes of related objects are not supported in DELETE queries!');
		}
		$where = $this->build_sql_where($this->get_filters());
		$where = $where ? "\n WHERE " . $where : '';
		if (!$where) throw new QueryBuilderException('Cannot perform update on all objects "' . $this->main_object->get_alias() . '"! Forbidden operation!');
		
		$query = 'DELETE FROM ' . $this->get_main_object()->get_data_address() . str_replace($this->get_main_object()->get_alias().'.', '', $where);
		$data_connection->run_sql($query);
		return $data_connection->get_affected_rows_count();;
	}
}
?>