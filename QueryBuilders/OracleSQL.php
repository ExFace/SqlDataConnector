<?php namespace exface\SqlDataConnector\QueryBuilders;

use exface\Core\DataTypes\AbstractDataType;
use exface\Core\CommonLogic\AbstractDataConnector;
/**
 * A query builder for oracle SQL.
 * 
 * @author aka
 *
 */
class OracleSQL extends AbstractSQL {
	// CONFIG
	protected $short_alias_max_length = 30; // maximum length of SELECT AS aliases
	protected $short_alias_remove_chars = array('.', '>', '<', '-', '(', ')', ':'); // forbidden chars in SELECT AS aliases
	protected $short_alias_forbidden = array('SIZE', 'SELECT', 'FROM', 'AS', 'PARENT', 'ID', 'LEVEL', 'ORDER', 'GROUP', 'COMMENT'); // forbidden SELECT AS aliases
	
	public function build_sql_query_select(){
		$filter_object_ids = array();
		$where = '';
		$group_by = '';
		$order_by = '';
		
		if ($this->get_limit()){
			// if the query is limited (pagination), run a core query for the filtering and pagination
			// and perform as many joins as possible afterwords only for the result of the core query
				
			$core_joins = array();
			$core_relations = array();
			$enrichment_selects = $this->get_attributes();
			$enrichment_select = '';
			$enrichment_joins = array();
			$enrichment_join = '';
			$enrichment_order_by = '';
			$core_selects = array();
			$core_select = '';
		
			// Build core query: join tables needed for filtering, grouping and sorting and select desired attributes from these tables
			// determine the needed JOINS
			// aggregations -> GROUP BY
			foreach ($this->get_aggregations() as $qpart){
				foreach ($qpart->get_used_relations() as $rel_alias => $rel){
					$core_relations[] = $rel_alias;
				}
				$core_joins = array_merge($core_joins, $this->build_sql_joins($qpart));
				$group_by .= ', ' . $this->build_sql_group_by($qpart);
			}
			// filters -> WHERE
			$where = $this->build_sql_where($this->get_filters());
			$core_joins = array_merge($core_joins, $this->build_sql_joins($this->get_filters()));
			$filter_object_ids = $this->get_filters()->get_object_ids_safe_for_aggregation();
			foreach ($this->get_filters()->get_used_relations() as $rel_alias => $rel){
				$core_relations[] = $rel_alias;
			}
			// sorters -> ORDER BY
			foreach ($this->get_sorters() as $qpart){
				if ($group_by){
					
				} else {
					foreach ($qpart->get_used_relations() as $rel_alias => $rel){
						$core_relations[] = $rel_alias;
					}
					$core_selects[$qpart->get_alias()] = $this->build_sql_select($qpart);
					$core_joins = array_merge($core_joins, $this->build_sql_joins($qpart));
					$order_by .= ', ' . $this->build_sql_order_by($qpart);
				}
				$enrichment_order_by .= ', ' . $this->build_sql_order_by($qpart, 'EXFCOREQ');
			}
				
			array_unique($core_relations);
				
			// separate core SELECTs from enrichment SELECTs
			foreach ($enrichment_selects as $nr => $qpart){
				if (in_array($qpart->get_attribute()->get_relation_path()->to_string(), $core_relations) || !$qpart->get_attribute()->get_relation_path()->to_string()){
					// Workaround to ensure, the UID is always in the query!
					// If we are grouping, we will not select any fields, that could be ambigous, thus
					// we can use MAX(UID), since all other values are the same within the group.
					if ($group_by 
					&& $qpart->get_alias() == $this->get_main_object()->get_uid_alias()
					&& !$qpart->get_aggregate_function()){
						$qpart->set_aggregate_function('MAX');
					}
					// If we are grouping, we can only select valid GROUP BY expressions from the core table.
					// These are either the ones with an aggregate function or thouse we are grouping by
					if ($group_by
					&& !$qpart->get_aggregate_function()
					&& !$this->get_aggregation($qpart->get_alias())){
						continue;
					}
					// also skip selects based on custom sql substatements if not being grouped over
					// they should be done after pagination as they are potentially very time consuming
					if ($this->check_for_sql_statement($qpart->get_attribute()->get_data_address()) && (!$group_by || !$qpart->get_aggregate_function())){
						continue;
					}
					// Also skip selects with reverse relations, as they will be fetched via subselects later
					// Selecting them in the core query would only slow it down. The filtering ist done explicitly in build_sql_where_condition()
					elseif ($qpart->get_used_relations('1n')){
						continue;
					} 
					// Add all remainig attributes of the core objects to the core query and select them 1-to-1 in the enrichment query
					else {
						$core_selects[$qpart->get_alias()] = $this->build_sql_select($qpart);
						$enrichment_select .= ', ' . $this->build_sql_select($qpart, 'EXFCOREQ', $this->get_short_alias($qpart->get_alias()), null, false);
					}
					unset($enrichment_selects[$nr]);
				} 
			}
			
			foreach ($enrichment_selects as $qpart){
				// If we are grouping, we can only join attributes of object instances, that are unique in the query
				// This is the case, if we filtered for exactly one instance of an object before, or if we aggregate
				// over this object or a relation to it.
				// TODO actually we need to make sure, the filter returns exactly one object, which probably means,
				// that the filter should be layed over an attribute, which uniquely identifies the object (e.g.
				// its UID column).
				if ($group_by
				&& in_array($qpart->get_attribute()->get_object()->get_id(), $filter_object_ids) === false){
					$related_to_aggregator = false;
					foreach ($this->get_aggregations() as $aggr){
						if (strpos($qpart->get_alias(), $aggr->get_alias()) === 0){
							$related_to_aggregator = true;
						}
					}
					if (!$related_to_aggregator){
						continue;
					}
				}
				// Check if we need some UIDs from the core tables to join the enrichments afterwards
				if ($first_rel = $qpart->get_first_relation()){
					if ($first_rel->get_type() == 'n1') {
						$first_rel_qpart = $this->add_attribute($first_rel->get_alias());
						// IDEA this does not support relations based on custom sql. Perhaps this needs to change
						$core_selects[$first_rel_qpart->get_attribute()->get_data_address()] = $this->build_sql_select($first_rel_qpart, null, null, $first_rel_qpart->get_attribute()->get_data_address(), ($group_by ? 'MAX' : null));
					}
				}
				
				// build the enrichment select. 
				if ($qpart->get_first_relation() && $qpart->get_first_relation()->get_type() == '1n'){
					// If the first relation needed for the select is a reverse one, make sure, the subselect will reference the core query directly
					$enrichment_select .= ', ' . $this->build_sql_select($qpart, 'EXFCOREQ');
				} else {
					// Otherwise the selects can rely on the joins
					$enrichment_select .= ', ' . $this->build_sql_select($qpart);
				}
				$enrichment_joins = array_merge($enrichment_joins, $this->build_sql_joins($qpart, 'exfcoreq'));
			}
				
			$core_select = str_replace(',,', ',', implode(',', $core_selects));
			$core_from = $this->build_sql_from();
			$core_join = implode(' ', $core_joins);
			$where = $where ? "\n WHERE " . $where : '';
			$group_by = $group_by ? ' GROUP BY ' . substr($group_by, 2) : '';
			$order_by = $order_by ? ' ORDER BY ' . substr($order_by, 2) : '';
		
			$enrichment_select = $enrichment_select ? str_replace(',,', ',', $enrichment_select) : '';
			$enrichment_select = substr($enrichment_select, 2);
			$enrichment_join = implode(' ', $enrichment_joins);
			$enrichment_order_by = $enrichment_order_by ? ' ORDER BY ' . substr($enrichment_order_by, 2) : '';
			$distinct = $this->get_select_distinct() ? 'DISTINCT ' : '';
		
			// build the query itself
			$core_query = "
								SELECT " . $distinct . $core_select . " FROM " . $core_from . $core_join . $where . $group_by . $order_by;
		
			$query =
			"\n SELECT " . $distinct . $enrichment_select . " FROM
				(SELECT *
					FROM
						(SELECT exftbl.*, ROWNUM EXFRN
							FROM (" . $core_query . ") exftbl
		         			WHERE ROWNUM <= " . ($this->get_limit() + $this->get_offset()) . "
						)
         			WHERE EXFRN > " . $this->get_offset() . "
         		) exfcoreq "
		         					. $enrichment_join . $enrichment_order_by;
		} else {
			// if there is no limit (no pagination), we just make a simple query
			$select = '';
			$joins = array();
			$join = '';
			$enrichment_select = '';
			$enrichment_joins = array();
			$enrichment_join = '';
			// WHERE
			$where = $this->build_sql_where($this->get_filters());
			$joins = $this->build_sql_joins($this->get_filters());
			$filter_object_ids = $this->get_filters()->get_object_ids_safe_for_aggregation();			
			$where = $where ? "\n WHERE " . $where : '';
			// GROUP BY
			foreach ($this->get_aggregations() as $qpart){
				$group_by .= ', ' . $this->build_sql_group_by($qpart);
			}
			$group_by = $group_by ? ' GROUP BY ' . substr($group_by, 2) : '';
			// SELECT
			foreach ($this->get_attributes() as $qpart){
				// if the query has a GROUP BY, we need to put the UID-Attribute in the core select as well as in the enrichment select
				// otherwise the enrichment joins won't work!
				if ($group_by && $qpart->get_attribute()->get_alias() === $qpart->get_attribute()->get_object()->get_uid_alias()){
					$select .= ', ' . $this->build_sql_select($qpart, null, null, null, 'MAX');
					$enrichment_select .= ', ' . $this->build_sql_select($qpart, 'EXFCOREQ');
				}
				// if we are aggregating, leave only attributes, that have an aggregate function,
				// and ones, that are aggregated over or can be assumed unique due to set filters
				elseif (!$group_by
						|| $qpart->get_aggregate_function()
						|| $this->get_aggregation($qpart->get_alias())){
					$select .= ', ' . $this->build_sql_select($qpart);
					$joins = array_merge($joins, $this->build_sql_joins($qpart));
				} elseif (in_array($qpart->get_attribute()->get_object()->get_id(), $filter_object_ids) !== false){
					$rels = $qpart->get_used_relations();
					$first_rel = false;
					if (!empty($rels)){
						$first_rel = reset($rels);
						$first_rel_qpart = $this->add_attribute($first_rel->get_alias());
						// IDEA this does not support relations based on custom sql. Perhaps this needs to change
						$select .= ', ' . $this->build_sql_select($first_rel_qpart, null, null, $first_rel_qpart->get_attribute()->get_data_address(), ($group_by ? 'MAX' : null));
					}
					$enrichment_select .= ', ' . $this->build_sql_select($qpart);
					$enrichment_joins = array_merge($enrichment_joins, $this->build_sql_joins($qpart, 'exfcoreq'));
					$joins = array_merge($joins, $this->build_sql_joins($qpart));
				}
		
			}
			$select = substr($select, 2);
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
				
			if (($group_by && $where) || $this->get_select_distinct()){
				$query = "\n SELECT " . $distinct . $enrichment_select . " FROM (SELECT " . $select . " FROM " . $from . $join . $where . $group_by . $order_by . ") EXFCOREQ " . $enrichment_join . $order_by;
			} else {
				$query = "\n SELECT " . $distinct .  $select . " FROM " . $from . $join . $where . $group_by . $order_by;
			}
		}
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
	
	protected function build_sql_select_null_check($select_statement, $value_if_null){
		return 'NVL(' . $select_statement . ', ' . (is_numeric($value_if_null) ? $value_if_null : '"' . $value_if_null . '"' ) . ')';
	}
	
	/**
	 * In oracle it seems, that the alias of the sort column should be in double quotes, whereas in other
	 * dialects (at least in MySQL), there the quotes prevent the sorting.
	 * FIXME Does not work with custom order by of attributes of related objects - only if sorting over a direct attribute of the main object. Autogenerated order by works fine.
	 * @param \exface\Core\CommonLogic\QueryBuilder\QueryPartSorter $qpart
	 * @return string
	 */
	protected function build_sql_order_by(\exface\Core\CommonLogic\QueryBuilder\QueryPartSorter $qpart, $select_from = null){
		if ($qpart->get_data_address_property("ORDER_BY")){
			$output = ($select_from ? $select_from : $this->get_short_alias($qpart->get_attribute()->get_relation_path()->to_string())) . '.' . $qpart->get_data_address_property("ORDER_BY");
		} else {
			$output = '"' . $this->get_short_alias($qpart->get_alias()) . '"';
		}
		$output .= ' ' . $qpart->get_order();
		return $output;
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
			case 'LIST':
				$output = "ListAgg(" . $select . ", " . ($args[0] ? $args[0] : "', '") . ") WITHIN GROUP (order by " . $select . ")";
				$qpart->get_query()->add_aggregation($qpart->get_attribute()->get_alias_with_relation_path());
				break;
			case 'LIST_DISTINCT':
				$output = "ListAggDistinct(" . $select . ")";
				$qpart->get_query()->add_aggregation($qpart->get_attribute()->get_alias_with_relation_path());
				break;
			default:
				break;
		}
		return $output;
	}
	
	public function create(AbstractDataConnector $data_connection = null){
		// If there are no values given for the main UID, use the primary key sequence from the data address properties of the main object
		if (!$this->get_value($this->get_main_object()->get_uid_alias())){
			// If there is no primary key sequence defined, try adding '_SEQ' to the table name. This seems to be a wide spread approach.
			// If this does not work, we will get an SQL error
			if (!$sequence = $this->get_main_object()->get_data_address_property('PKEY_SEQUENCE')){
				$sequence = $this->get_main_object()->get_data_address() . '_SEQ';
			}
			$this->add_value($this->get_main_object()->get_uid_alias(), $sequence . '.NEXTVAL');
		}
		return parent::create($data_connection);
	}
	
	protected function prepare_input_value($value, AbstractDataType $data_type, $sql_data_type = NULL){
		if ($data_type->is(EXF_DATA_TYPE_DATE)
		|| $data_type->is(EXF_DATA_TYPE_TIMESTAMP)){
			$value = "TO_DATE('" . $this->escape_string($value) . "', 'yyyy-mm-dd hh24:mi:ss')";
		} else {
			$value = parent::prepare_input_value($value, $data_type, $sql_data_type);
		}
		return $value;
	}
	
	protected function prepare_where_value($value, AbstractDataType $data_type, $sql_data_type = NULL){
		if ($data_type->is(EXF_DATA_TYPE_DATE)
		|| $data_type->is(EXF_DATA_TYPE_TIMESTAMP)){
			$output = "TO_DATE('" . $value . "', 'yyyy-mm-dd hh24:mi:ss')";
		} else {
			$output = parent::prepare_where_value($value, $data_type);
		}
		return $output;
	}
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\SqlDataConnector\QueryBuilders\AbstractSQL::escape_string()
	 */
	protected function escape_string($string){
		return str_replace("'", "''", $string);
	}
}
?>