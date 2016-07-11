<?php
namespace exface\SqlDataConnector\QueryBuilders;

/**
 * A query builder for oracle SQL.
 * 
 * @author aka
 *
 */
class MsSQL extends AbstractSQL {
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
		$group_safe_attribute_aliases = array();
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
		
		$has_attributes_with_reverse_relations = count($this->get_attributes_with_reverse_relations());
		
		// GROUP BY
		foreach ($this->get_aggregations() as $qpart){
			$group_by .= ', ' . $this->build_sql_group_by($qpart, ($has_attributes_with_reverse_relations ? 'EXFCOREQ' : null));
		}
		$group_by = $group_by ? ' GROUP BY ' . substr($group_by, 2) : '';
		
		// If there is a limit in the query, ensure there is an ORDER BY even if no sorters given. If not, add one over the UID
		if (sizeof($this->get_sorters()) < 1 && $this->get_limit()){
			if ($group_by){
				$order_by .= ', EXFCOREQ.' . $this->get_main_object()->get_uid_attribute()->get_data_address() . ' DESC';
			} else {
				$order_by .= ', ' . $this->get_main_object()->get_uid_attribute()->get_data_address() . ' DESC';
			}
		}
		
		// SELECT
		/*	@var $qpart \exface\Core\CommonLogic\QueryBuilder\QueryPartSelect */
		foreach ($this->get_attributes() as $qpart){
			$skipped = false;
			// if the query has a GROUP BY, we need to put the UID-Attribute in the core select as well as in the enrichment select
			// otherwise the enrichment joins won't work!
			if ($group_by 
				&& $qpart->get_attribute()->get_alias() === $qpart->get_attribute()->get_object()->get_uid_alias()
				&& !$has_attributes_with_reverse_relations){
				$selects[] = $this->build_sql_select($qpart, null, null, null, 'MAX');
				$enrichment_select .= ', ' . $this->build_sql_select($qpart, 'EXFCOREQ', $qpart->get_attribute()->get_object()->get_uid_alias());
				$group_safe_attribute_aliases[] = $qpart->get_attribute()->get_alias_with_relation_path();
			}
			// if we are aggregating, leave only attributes, that have an aggregate function,
			elseif (!$group_by
					|| $qpart->get_aggregate_function()
					|| $this->get_aggregation($qpart->get_alias())){
				$selects[] = $this->build_sql_select($qpart);
				$joins = array_merge($joins, $this->build_sql_joins($qpart));
				$group_safe_attribute_aliases[] = $qpart->get_attribute()->get_alias_with_relation_path();
			// ...and ones, that are aggregated over or can be assumed unique due to set filters
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
				$group_safe_attribute_aliases[] = $qpart->get_attribute()->get_alias_with_relation_path();
			// ...and ones, that belong directly to objects, we are aggregating over (they can be assumed unique too, since their object is unique per row)
			} elseif ($group_by && $this->get_aggregation($qpart->get_attribute()->get_relation_path()->to_string())){
				$selects[] = $this->build_sql_select($qpart, null, null, null, 'MAX');
				$joins = array_merge($joins, $this->build_sql_joins($qpart));
				$group_safe_attribute_aliases[] = $qpart->get_attribute()->get_alias_with_relation_path();
			} else {
				$skipped = true;
			}
			
			// If we have attributes, that need reverse relations, we must move the group by to the outer (enrichment) query, because
			// the subselects of the subqueries will reference UIDs of the core rows, thus making grouping in the core query impossible
			if (!$skipped && $group_by && $has_attributes_with_reverse_relations){
				$enrichment_select .= ', ' . $this->build_sql_select($qpart, 'EXFCOREQ', $this->get_short_alias($qpart->get_alias()));
			}
	
		}
		$select = implode(', ', array_unique($selects));
		if ($group_by && $has_attributes_with_reverse_relations){
			$enrichment_select = substr($enrichment_select, 2);
		} else {
			$enrichment_select = 'EXFCOREQ.*' . ($enrichment_select ? ', ' . substr($enrichment_select, 2) : '');
		}
		
		// FROM
		$from = $this->build_sql_from();
		
		// JOINs
		$join = implode(' ', $joins);
		$enrichment_join = implode(' ', $enrichment_joins);
		
		// ORDER BY
		foreach ($this->get_sorters() as $qpart){
			// A sorter can only be used, if there is no GROUP BY, or the sorted attribute has unique values within the group
			if (!$this->get_aggregations() || in_array($qpart->get_attribute()->get_alias_with_relation_path(), $group_safe_attribute_aliases)){
				$order_by .= ', ' . $this->build_sql_order_by($qpart);
			}
		}
		$order_by = $order_by ? ' ORDER BY ' . substr($order_by, 2) : '';
		
		$distinct = $this->get_select_distinct() ? 'DISTINCT ' : '';
		
		if ($this->get_limit()){
			$limit = ' OFFSET ' . $this->get_offset() . ' ROWS FETCH NEXT ' . $this->get_limit() . ' ROWS ONLY';
		}
			
		if (($group_by && ($where || $has_attributes_with_reverse_relations)) || $this->get_select_distinct()){
			if (count($this->get_attributes_with_reverse_relations()) > 0){
				$query = "\n SELECT " . $distinct . $enrichment_select . " FROM (SELECT " . $select . " FROM " . $from . $join . $where . ") EXFCOREQ " . $enrichment_join . $group_by . $order_by . $limit;
			} else {
				$query = "\n SELECT " . $distinct . $enrichment_select . " FROM (SELECT " . $select . " FROM " . $from . $join . $where . $group_by . ") EXFCOREQ " . $enrichment_join . $order_by . $limit;
			}
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
			$aggregators = $this->get_aggregations();
			$totals_core_select = $this->build_sql_select(reset($aggregators));
		}
		
		if ($totals_core_select){
			$totals_query = "\n SELECT COUNT(*) AS EXFCNT " . $totals_select . " FROM (SELECT " . $totals_core_select . ' FROM ' . $totals_from . $totals_join . $totals_where . $totals_group_by . ") EXFCOREQ";
		} else {
			$totals_query = "\n SELECT COUNT(*) AS EXFCNT FROM " . $totals_from . $totals_join . $totals_where . $totals_group_by;
		}
		return $totals_query;
	}
	
	protected function build_sql_select_null_check($select_statement, $value_if_null){
		return 'ISNULL(' . $select_statement . ', ' . (is_numeric($value_if_null) ? $value_if_null : '"' . $value_if_null . '"' ) . ')';
	}
}
?>