<?php namespace exface\SqlDataConnector\QueryBuilders;

use exface\Core\Exceptions\QueryBuilderException;
use exface\Core\CommonLogic\QueryBuilder\AbstractQueryBuilder;
use exface\Core\CommonLogic\QueryBuilder\QueryPartFilterGroup;
use exface\Core\CommonLogic\QueryBuilder\QueryPartAttribute;
use exface\Core\CommonLogic\QueryBuilder\QueryPartFilter;
use exface\Core\DataTypes\AbstractDataType;
use exface\Core\CommonLogic\AbstractDataConnector;
use exface\Core\CommonLogic\Model\RelationPath;
use exface\Core\Exceptions\DataValidationException;
use exface\Core\Exceptions\DataTypeValidationError;
use exface\Core\DataTypes\NumberDataType;

/**
 * A query builder for oracle SQL.
 * 
 * @author Andrej Kabachnik
 *
 */
abstract class AbstractSQL extends AbstractQueryBuilder{
	// CONFIG
	protected $short_alias_max_length = 30; // maximum length of SELECT AS aliases
	protected $short_alias_remove_chars = array('.', '>', '<', '-', '(', ')', ':'); // forbidden chars in SELECT AS aliases
	protected $short_alias_forbidden = array('SIZE', 'SELECT', 'FROM', 'AS', 'PARENT', 'ID', 'LEVEL', 'ORDER'); // forbidden SELECT AS aliases
	
	// other vars
	protected $select_distinct = false;
	protected $short_aliases = array();
	protected $short_alias_index = 0;
	/** [ [column_name => column_value] ]*/
	protected $result_rows = array();
	/** [ [column_name => column_value] ] having multiple rows if multiple totals for a single column needed */
	protected $result_totals = array();
	protected $result_total_count = 0;
	
	private $binary_columns = array();
	private $query_id = null;
	private $subquery_counter = 0;
	
	public function get_select_distinct() {
		return $this->select_distinct;
	}
	
	public function set_select_distinct($value) {
		$this->select_distinct = $value;
	}  
	
	abstract function build_sql_query_select();
	abstract function build_sql_query_totals();
	
	public function read(AbstractDataConnector $data_connection = null){
		if (!$data_connection) $data_connection = $this->get_main_object()->get_data_connection();	
		
		$query = $this->build_sql_query_select();
		// first do the main query
		if ($rows = $data_connection->run_sql($query)->get_result_array()){
			foreach ($this->get_binary_columns() as $full_alias){
				$short_alias = $this->get_short_alias($full_alias);
				foreach ($rows as $nr => $row){
					$rows[$nr][$full_alias] = $this->decode_binary($row[$short_alias]);
				}
			}
			// TODO filter away the EXFRN column!
			foreach ($this->short_aliases as $short_alias){
				$full_alias = $this->get_full_alias($short_alias);
				foreach ($rows as $nr => $row){
					$rows[$nr][$full_alias] = $row[$short_alias];
					unset($rows[$nr][$short_alias]);
				}
			}
				
			$this->result_rows = $rows;
		}
		
		// then do the totals query if needed
		$totals_query = $this->build_sql_query_totals();
		if ($totals = $data_connection->run_sql($totals_query)->get_result_array()){
			// the total number of rows is treated differently, than the other totals.
			$this->result_total_count = $totals[0]['EXFCNT'];
			// now save the custom totals.
			foreach ($this->totals as $qpart){
				$this->result_totals[$qpart->get_row()][$qpart->get_alias()] = $totals[0][$this->get_short_alias($qpart->get_alias())];
			}
		}
		return count($this->result_rows);
	}
	
	function get_result_rows(){
		return $this->result_rows;
	}

	function get_result_totals(){
		return $this->result_totals;
	}

	function get_result_total_rows(){
		return $this->result_total_count;
	}
	
	/**
	 * Checks if writing operations (create, update, delete) are possible for the current query.
	 * @return boolean
	 */
	protected function is_writable(){
		$result = true;
		// First of all find out, if the object's data address is empty or a view. If so, we generally can't write to it!
		if (!$this->get_main_object()->get_data_address()){
			throw new QueryBuilderException('The data address of the object "' . $this->get_main_object()->get_alias() . '" is empty. Cannot perform writing operations!');
			$result = false;;
		}
		if ($this->check_for_sql_statement($this->get_main_object()->get_data_address())){
			throw new QueryBuilderException('The data address of the object "' . $this->get_main_object()->get_alias() . '" seems to be a view. Cannot write to SQL views!');
			$result = false;
		}
		
		return $result;
	}
	
	/**
	 * (non-PHPdoc)
	 * @see \exface\Core\CommonLogic\QueryBuilder\AbstractQueryBuilder::create()
	 */
	function create(AbstractDataConnector $data_connection = null){
		if (!$data_connection) $data_connection = $this->main_object->get_data_connection();
		if (!$this->is_writable()) return 0;
		$insert_ids = array();
		
		$values = array();
		$columns = array();
		$uid_qpart = null;
		// add values
		foreach ($this->get_values() as $qpart){
			$attr = $qpart->get_attribute();
			if ($attr->get_relation_path()->to_string()) {
				throw new QueryBuilderException('Cannot create attribute "' . $attr->get_alias() . '" of object "' . $this->get_main_object()->get_alias() . '". Attributes of related objects cannot be created within the same SQL query!');
				continue;
			}
			// Ignore attributes, that do not reference an sql column (= do not have a data address at all)
			if (!$attr->get_data_address() || $this->check_for_sql_statement($attr->get_data_address())) {
				continue;
			} 
			// Save the query part for later processing if it is the object's UID
			if ($attr->is_uid_for_object()){
				$uid_qpart = $qpart;
			}
			
			// Prepare arrays with column aliases and values to implode them later when building the query
			// Make sure, every column is only addressed once! So the keys of both array actually need to be the column aliases
			// to prevent duplicates
			$columns[$attr->get_data_address()] = $attr->get_data_address();	
			foreach ($qpart->get_values() as $row => $value){
				$values[$row][$attr->get_data_address()] = $this->prepare_input_value($value, $attr->get_data_type(), $attr->get_data_address_property('SQL_DATA_TYPE'));
			}
		}
		
		// If there is no UID column, but the UID attribute has a custom insert statement, add it at this point manually
		// This is important because the UID will mostly not be marked as a mandatory attribute in order to preserve the
		// possibility of mixed creates and updates among multiple rows. But an empty non-required attribute will never
		// show up as a value here. Still that value is required!
		if (is_null($uid_qpart) && $uid_generator = $this->get_main_object()->get_uid_attribute()->get_data_address_property('SQL_INSERT')){
			$last_uid_sql_var = '@last_uid';
			$columns[] = $this->get_main_object()->get_uid_attribute()->get_data_address();
			foreach ($values as $nr => $row){
				$values[$nr][] = $last_uid_sql_var . ' := ' . $uid_generator;
			}
		}
		
		foreach ($values as $nr => $row){
			foreach ($row as $val){
				$values[$nr] = implode(',', $row);
			}
		}
		
		$query = 'INSERT INTO ' . $this->get_main_object()->get_data_address() . ' (' . implode(', ', $columns) . ') VALUES (' . implode('), (' , $values) . ')';
		$data_connection->run_sql($query);
		
		// Now get the primary key of the last insert.
		if ($last_uid_sql_var){
			// If the primary key was a custom generated one, it was saved to the corresponding SQL variable.
			// Fetch it from the data base
			$last_id = reset($data_connection->run_sql('SELECT CONCAT(\'0x\', HEX(' . $last_uid_sql_var . '))')->get_result_array()[0]);
		} else {
			// If the primary key was autogenerated, fetch it via built-in function
			$last_id = $data_connection->get_insert_id();
		}
		$affected_rows = $data_connection->get_affected_rows_count();
		// TODO How to get multipla inserted ids???
		if ($affected_rows){
			$insert_ids[] = $last_id;	
		}
		return $insert_ids;
	}
	
	/**
	 * Performs SQL update queries. Depending on the number of rows to be updated, there will be one or more queries performed. 
	 * Theoretically attributes with one and multiple value rows can be mixed in one QueryBuilder instance: e.g. some attributes
	 * need different values per row and others are set to a single value for all rows matching the filter criteria. In this case
	 * there will be one SQL query to update all single-value-attribtues and potentially multiple queries to update attributes by
	 * row. The latter queries will only have the respecitve primary key in their WHERE clause, whereas the single-value-query 
	 * will have the filters from the QueryBuilder.
	 * 
	 * In any case, direct updates are only performed on attributes of the main meta object. If an update of a related attribute
	 * is needed, a separate update query for the meta object of that attribute will be created and will get executed after the
	 * main query. Subqueries are executed in the order in which the respective attributes were added to the QueryBuilder. 
	 * 
	 * (non-PHPdoc)
	 * @see \exface\Core\CommonLogic\QueryBuilder\AbstractQueryBuilder::update()
	 */
	function update(AbstractDataConnector $data_connection = null){
		if (!$data_connection) $data_connection = $this->main_object->get_data_connection();
		if (!$this->is_writable()) return 0;
		
		// Filters -> WHERE
		// Since UPDATE queries generally do not support joins, tell the build_sql_where() method not to rely on joins in the main query 
		$where = $this->build_sql_where($this->get_filters(), false);
		$where = $where ? "\n WHERE " . $where : '';
		if (!$where) {
			throw new QueryBuilderException('Cannot perform update on all objects "' . $this->get_main_object()->get_alias() . '"! Forbidden operation!');
		}
		
		// Attributes -> SET	
		
		// Array of SET statements for the single-value-query which updates all rows matching the given filters
		// [ 'data_address = value' ]
		$updates_by_filter = array(); 
		// Array of SET statements to update multiple values per attribute. They will be used to build one UPDATE statement per UID value
		// [ uid_value => [ data_address => 'data_address = value' ] ]
		$updates_by_uid = array(); 
		// Array of query parts to be placed in subqueries
		$subqueries_qparts = array(); 
		foreach ($this->get_values() as $qpart){
			$attr = $qpart->get_attribute();
			if ($attr->get_relation_path()->to_string()) {
				$subqueries_qparts[] = $qpart;
				continue;
			}
			
			// Ignore attributes, that do not reference an sql column (= do not have a data address at all)
			if ($this->check_for_sql_statement($attr->get_data_address())) {
				continue;
			}
				
			if (count($qpart->get_values()) == 1){
				$values = $qpart->get_values();
				$value = $this->prepare_input_value(reset($values), $attr->get_data_type(), $attr->get_data_address_property('SQL_DATA_TYPE'));
				$updates_by_filter[] = $this->get_main_object()->get_alias() . '.' . $attr->get_data_address() . ' = ' . $value;
			} else {
				// TODO check, if there is an id for each value. Those without ids should be put into another query to make an insert
				//$cases = '';
				if (count($qpart->get_uids()) == 0){
					throw new QueryBuilderException('Cannot update attribute "' . $qpart->get_alias() . "': no UIDs for rows to update given!");
				}
				foreach ($qpart->get_values() as $row_nr => $value){
					$value = $this->prepare_input_value($value, $attr->get_data_type(), $attr->get_data_address_property('SQL_DATA_TYPE'));
					/* IDEA In earlier versions multi-value-updates generated a single query with a CASE statement for each attribute. 
					 * This worked find for smaller numbers of values (<50) but depleted the memory with hundreds of values per attribute. 
					 * A quick fix was to introduce separate queries per value. But it takes a lot of time to fire 1000 separate queries. 
					 * So we could mix the two approaches and make separate queries every 10-30 values with fairly short CASE statements.
					 * This would shorten the number of queries needed by factor 10-30, but it requires the separation of values of all
					 * participating attributes into blocks sorted by UID. In other words, the resulting queries must have all values for
					 * the UIDs they address and a new filter with exactly this list of UIDs.
					 */
					//$cases[$qpart->get_uids()[$row_nr]] = 'WHEN ' . $qpart->get_uids()[$row_nr] . ' THEN ' . $value . "\n";
					$updates_by_uid[$qpart->get_uids()[$row_nr]][$this->get_main_object()->get_alias() . '.' . $attr->get_data_address()] = $this->get_main_object()->get_alias() . '.' . $attr->get_data_address() .' = '. $value;
				}
				// See comment about CASE-based updates a few lines above
				//$updates_by_filter[] = $this->get_main_object()->get_alias() . '.' . $attr->get_data_address() . " = CASE " . $this->get_main_object()->get_uid_attribute()->get_data_address() . " \n" . implode($cases) . " END";
			}
		}
		
		// Execute the main query
		foreach ($updates_by_uid as $uid => $row){
			$query = 'UPDATE ' . $this->build_sql_from() . ' SET ' . implode(', ', $row) . ' WHERE ' . $this->get_main_object()->get_uid_attribute()->get_data_address() . '=' . $uid;
			$data_connection->run_sql($query);
			$affected_rows += $data_connection->get_affected_rows_count();
		}
		
		if (count($updates_by_filter) > 0){
			$query = 'UPDATE ' . $this->build_sql_from() . ' SET ' . implode(', ', $updates_by_filter) . $where;
			$data_connection->run_sql($query);
			$affected_rows = $data_connection->get_affected_rows_count();
		}
		
		// Execute Subqueries
		foreach ($this->split_by_meta_object($subqueries_qparts) as $subquery){
			$subquery->update($data_connection);
		}
				
		return $affected_rows;
	}
	
	/**
	 * Splits the a seta of query parts of the current query into multiple separate queries, each of them containing only query 
	 * parts with direct attributes of one single object.
	 *
	 * For example, concider a query for the object ORDER with the following attributes, values, or whatever other query parts: 
	 * NUMBER, DATE, CUSTOMER->NAME, DELIVER_ADDRESS->STREET, DELIVERY_ADDRESS->NO. A split would give you two queries: one for 
	 * ORDER (with the columns NUMBER and DATE) and one for ADDRESS (with the columns STREET and NO).
	 * @param QueryPartAttribute[] $qparts
	 * @return sql_AbstractSQL[]
	 */
	protected function split_by_meta_object(array $qparts){
		$queries = array();
		foreach ($qparts as $qpart){
			/* @var $attr \exface\Core\CommonLogic\Model\attribute */
			$attr = $qpart->get_attribute();
			if (!$queries[$attr->get_relation_path()->to_string()]){
				$q = clone $this;
				if ($attr->get_relation_path()->to_string()){
					$q->set_main_object($this->get_main_object()->get_related_object($attr->get_relation_path()->to_string()));
					$q->set_filters_condition_group($this->get_filters()->get_condition_group()->rebase($attr->get_relation_path()->to_string()));
				} else {
					$q->set_filters($this->get_filters());
				}
				$q->clear_values();
				$queries[$attr->get_relation_path()->to_string()] = $q;
				unset($q);
			}
			$queries[$attr->get_relation_path()->to_string()]->add_query_part($qpart->rebase($queries[$attr->get_relation_path()->to_string()], $attr->get_relation_path()->to_string()));
		}
		return $queries;
	}
	
	/**
	 * Escapes a given value in the proper way for it's data type. The result can be safely used in INSERT
	 * or UPDATE queries.
	 * IDEA create a new qpart for input values and use it as an argument in this method. Only need one argument then.
	 * @param multitype $value
	 * @param AbstractDataType $data_type
	 * @param string $sql_data_type
	 * @return string
	 */
	protected function prepare_input_value($value, AbstractDataType $data_type, $sql_data_type = NULL){
		
		if ($data_type->is(EXF_DATA_TYPE_STRING)){
			$value = "'" . $this->escape_string($value) . "'";
		} elseif ($data_type->is(EXF_DATA_TYPE_BOOLEAN)){
			$value = ($value == "false" || $value == "FALSE" || !$value ? 0 : 1);
		} elseif ($data_type->is(EXF_DATA_TYPE_NUMBER)){
			$value = ($value == '' ? 'NULL' : $value ); 
		} elseif ($data_type->is(EXF_DATA_TYPE_DATE)){
			if (!$value){
				$value = 'NULL';
			} else {
				$value = "'" . $this->escape_string($value) . "'";
			}
		} elseif ($data_type->is(EXF_DATA_TYPE_RELATION)){
			if ($value == ''){
				$value = 'NULL';
			} else {
				$value = NumberDataType::validate($value) ? $value : "'" . $this->escape_string($value) . "'";
			}
		} else {
			$value = "'" . $this->escape_string($value) . "'";
		}
		return $value;
	}
	
	/**
	 * Escapes a given string in order to use it in sql queries
	 * @param string $string
	 * @return string
	 */
	protected function escape_string($string){
		return addslashes($string);
	}
	
	/**
	 * (non-PHPdoc)
	 * @see \exface\Core\CommonLogic\QueryBuilder\AbstractQueryBuilder::delete()
	 */
	function delete(AbstractDataConnector $data_connection = null){
		// filters -> WHERE
		// Relations (joins) are not supported in delete clauses, so check for them first!
		if (count($this->get_filters()->get_used_relations()) > 0){
			throw new QueryBuilderException('Filters over attributes of related objects ("' . $attribute . '") are not supported in DELETE queries!');
		}
		$where = $this->build_sql_where($this->get_filters());
		$where = $where ? "\n WHERE " . $where : '';
		if (!$where){
			throw new QueryBuilderException('Cannot delete all data from "' . $this->main_object->get_alias() . '". Forbidden operation!');
		}
		
		$query = 'DELETE FROM ' . $this->build_sql_from() . $where;
		$data_connection->run_sql($query);
		
		return $data_connection->get_affected_rows_count();
	}
	
	/**
	 * Creats a SELECT statement for an attribute (qpart).
	 * The parameters override certain parts of the statement: $group_function( $select_from.$select_column AS $select_as ).
	 * Set parameters to null to disable them. Other values (like '') do not disable them!
	 * 
	 * TODO multiple reverse relations in line cause trouble, as the group by only groups the last of them, not the ones
	 * in the middle. A possible solutions would be joining the tables starting from the last reverse relation in line back to
	 * the first one.
	 * Bad: 
	 * 		(SELECT 
  	 *			(SELECT SUM(POS_TRANSACTIONS.AMOUNT) AS "SALES_QTY_SUM1" 
     *				FROM PCD_TABLE POS_TRANSACTIONS
     *				WHERE  POS_TRANSACTIONS.ARTICLE_IDENT = ARTI.OID
   	 *			) AS "POS_TRANSACTIONS__SALES_QTY_S1" 
	 *		FROM ARTICLE_IDENT ARTI
 	 *		WHERE  ARTI.ARTICLE_COLOR_OID = EXFCOREQ.OID
 	 *		) AS "ARTI__POS_TRANSACTIONS__SALES1"
	 * Good:
	 * 		(SELECT SUM(POS_TRANSACTIONS.AMOUNT) AS "SALES_QTY_SUM1" 
  	 *			FROM PCD_TABLE POS_TRANSACTIONS
   	 *			LEFT JOIN ARTICLE_IDENT ARTI ON POS_TRANSACTIONS.ARTICLE_IDENT = ARTI.OID
  	 *			WHERE ARTI.ARTICLE_COLOR_OID = EXFCOREQ.OID) AS "ARTI__POS_TRANSACTIONS__SALES1"
  	 * Another idea might be to enforce grouping after every reverse relation. Don't know, how it would look like in SQL though...
	 * 
	 * @param \exface\Core\CommonLogic\QueryBuilder\QueryPart $qpart
	 * @param string $select_from
	 * @param string $select_column
	 * @param string $select_as set to false or '' to remove the "AS xxx" part completely
	 * @param string $group_function set to false or '' to remove grouping completely
	 * @return string
	 */
	/**
	 * Creats a SELECT statement for an attribute (qpart).
	 * The parameters override certain parts of the statement: $group_function( $select_from.$select_column AS $select_as ).
	 * Set parameters to null to disable them. Other values (like '') do not disable them!
	 *
	 * TODO multiple reverse relations in line cause trouble, as the group by only groups the last of them, not the ones
	 * in the middle. A possible solutions would be joining the tables starting from the last reverse relation in line back to
	 * the first one.
	 * Bad:
	 * 		(SELECT
	 *			(SELECT SUM(POS_TRANSACTIONS.AMOUNT) AS "SALES_QTY_SUM1"
	 *				FROM PCD_TABLE POS_TRANSACTIONS
	 *				WHERE  POS_TRANSACTIONS.ARTICLE_IDENT = ARTI.OID
	 *			) AS "POS_TRANSACTIONS__SALES_QTY_S1"
	 *		FROM ARTICLE_IDENT ARTI
	 *		WHERE  ARTI.ARTICLE_COLOR_OID = EXFCOREQ.OID
	 *		) AS "ARTI__POS_TRANSACTIONS__SALES1"
	 * Good:
	 * 		(SELECT SUM(POS_TRANSACTIONS.AMOUNT) AS "SALES_QTY_SUM1"
	 *			FROM PCD_TABLE POS_TRANSACTIONS
	 *			LEFT JOIN ARTICLE_IDENT ARTI ON POS_TRANSACTIONS.ARTICLE_IDENT = ARTI.OID
	 *			WHERE ARTI.ARTICLE_COLOR_OID = EXFCOREQ.OID) AS "ARTI__POS_TRANSACTIONS__SALES1"
	 * Another idea might be to enforce grouping after every reverse relation. Don't know, how it would look like in SQL though...
	 *
	 * @param \exface\Core\CommonLogic\QueryBuilder\QueryPart $qpart
	 * @param string $select_from
	 * @param string $select_column
	 * @param string $select_as set to false or '' to remove the "AS xxx" part completely
	 * @param string $group_function set to false or '' to remove grouping completely
	 * @return string
	 */
	protected function build_sql_select(\exface\Core\CommonLogic\QueryBuilder\QueryPart $qpart, $select_from = null, $select_column = null, $select_as = null, $group_function = null){
		$output = '';
		$add_nvl = false;
		$attribute = $qpart->get_attribute();
	
		// skip attributes with no select (e.g. calculated from other values via formatters)
		if (!$attribute->get_data_address()) return;
			
		if (!$select_from) {
			// if it's a relation, we need to select from a joined table except for reverse relations
			if ($select_from = $attribute->get_relation_path()->to_string()){
				if ($rev_rel = $qpart->get_first_relation('1n')){
					// In case of reverse relations, $select_from is used to connect the subselects.
					// Here we use the table of the last regular relation relation before the reversed one.
					$select_from = RelationPath::relaton_path_cut($select_from, null, $rev_rel->get_alias());
				}
			}
			// otherwise select from the main table
			if (!$select_from){
				$select_from = $this->main_object->get_alias();
			}
			$select_from .= $this->get_query_id();
		}
		if (is_null($select_as)) $select_as = $qpart->get_alias();
		$select_from = $this->get_short_alias($select_from);
		$select_as = $this->get_short_alias($select_as);
		$group_function = !is_null($group_function) ? $group_function : $qpart->get_aggregate_function();
	
		// build subselects for reverse relations if the body of the select is not specified explicitly
		if (!$select_column && $qpart->get_used_relations('1n')){
			$output = $this->build_sql_select_subselect($qpart, $select_from);
			$add_nvl = true;
		}
		// build grouping function if necessary
		elseif ($group_function) {
			$output = $this->build_sql_group_function($qpart, $select_from, $select_column, $select_as, $group_function);
			$add_nvl = true;
		}
		// otherwise create a regular select
		else {
			if ($select_column){
				// if the column to select is explicitly defined, just select it
				$output = $select_from . '.' . $select_column;
			} elseif ($this->check_for_sql_statement($attribute->get_data_address())){
				// see if the attribute is a statement.
				// set aliases
				$output = '(' . str_replace(array('[#alias#]'), $select_from, $attribute->get_data_address()) . ')';
			} else {
				// otherwise get the select from the attribute
				$output = $select_from . '.' . $attribute->get_data_address();
			}
		}
	
		if ($add_nvl){
			// do some prettyfying
			// return zero for number fields if the subquery does not return anything
			if ($attribute->get_data_type()->is(EXF_DATA_TYPE_NUMBER)){
				$output = $this->build_sql_select_null_check($output, 0);
			}
		}
	
		if ($select_as){
			$output = "\n" . $output . ' AS "' . $select_as . '"';
		}
		return $output;
	}
	
	/**
	 * Adds a wrapper to a select statement, that should take care of the returned value if the statement
	 * itself returns null (like IFNULL(), NVL() or COALESCE() depending on the SQL dialect).
	 * @param string $select_statement
	 * @param string $value_if_null
	 * @return string
	 */
	protected function build_sql_select_null_check($select_statement, $value_if_null){
		return 'COALESCE(' . $select_statement . ', ' . (is_numeric($value_if_null) ? $value_if_null : '"' . $value_if_null . '"' ) . ')';
	}
	
	/**
	 * Builds subselects for reversed relations
	 * @param \exface\Core\CommonLogic\QueryBuilder\QueryPart $qpart
	 * @param string $select_from
	 * @param string $select_column
	 * @param string $select_as
	 * @param string $group_function
	 * @return string
	 */
	protected function build_sql_select_subselect(\exface\Core\CommonLogic\QueryBuilder\QueryPart $qpart, $select_from = null){
		$rev_rel = $qpart->get_first_relation('1n');
		if (!$rev_rel) return '';
		
		/* if there is at least one reverse relation, we need to build a subselect. This is a bit tricky since
		 * "normal" and reverse relations can be mixed in the chain of relations for a certain attribute. Imagine,
		* we would like to see the customer card number and type in a list of orders. Assuming the customer may
		* have multiple cards we get the following: ORDER->CUSTOMER<->CUSTOMER_CARD->TYPE->LABEL. Thus we need to
		* join ORDER and CUSTOMER in the main query and create a subselect for CUSTOMER_CARD joined with TYPE.
		* The subselect needs to be filtered by ORDER.CUSTOMER_ID which is the foriegn key of CUSTOMER. We will
		* reference this example in the comments below.
		*/
		/** @var string part of the relation part up to the first reverse relation */
		$reg_rel_path = RelationPath::relaton_path_cut($qpart->get_alias(), null, $rev_rel->get_alias());
		/** @var string complete path of the first reverse relation */
		$rev_rel_path = RelationPath::relation_path_add($reg_rel_path, $rev_rel->get_alias());
	
		// build a subquery
		/* @var $relq \exface\SqlDataConnector\QueryBuilders\AbstractSQL */
		$qb_class = get_class($this);
		// TODO Use QueryBuilderFactory here instead
		$relq = new $qb_class;
		// the query is based on the first object after the reversed relation (CUSTOMER_CARD for the above example)
		$relq->set_main_object($rev_rel->get_related_object());
		$relq->set_query_id($this->get_next_subquery_id());
	
		// Add the key alias relative to the first reverse relation (TYPE->LABEL for the above example)
		$relq->add_attribute(RelationPath::relaton_path_cut($qpart->get_alias(), $rev_rel->get_alias()));
	
		// Set the filters of the subquery to all filters of the main query, that need to be applied to objects beyond the reverse relation. 
		// In our examplte, those would be any filter on ORDER->CUSTOMER<-CUSTOMER_CARD or ORDER->CUSTOMER<-CUSTOMER_CARD->TYPE, etc. Filters
		// over ORDER oder ORDER->CUSTOMER would be applied to the base query and ar not neeede in the subquery any more.
		// If we rebase and add all filters, it will still work, but the SQL would get much more complex and surely slow with large data sets.
		// Set $remove_conditions_not_matching_the_path parameter to true, to make sure, only applicable filters will get rebased.
		$relq->set_filters_condition_group($this->get_filters()->get_condition_group()->rebase($rev_rel_path, true));
		// Add a new filter to attach to the main query (WHERE CUSTOMER_CARD.CUSTOMER_ID = ORDER.CUSTOMER_ID for the above example)
		// This only makes sense, if we have a reference to the parent query (= the $select_from parameter is set)
		if ($select_from){
			if ($reg_rel_path){ 
				// attach to the related object key of the last regular relation before the reverse one
				$junction_attribute = $this->get_main_object()->get_attribute(RelationPath::relation_path_add($reg_rel_path, $this->get_main_object()->get_relation($reg_rel_path)->get_related_object_key_alias()));
				$junction = $junction_attribute->get_data_address();
			} else { // attach to the uid of the core query if there are no regular relations preceeding the reversed one
				$junction = $this->get_main_object()->get_uid_attribute()->get_data_address();
			}
			// The filter needs to be an EQ, since we want a to compare by "=" to whatever we define without any quotes
			// Putting the value in brackets makes sure it is treated as an SQL expression and not a normal value
			$relq->add_filter_from_string($rev_rel->get_foreign_key_alias(), '(' . $select_from . '.' . $junction . ')', EXF_COMPARATOR_EQUALS);
		}
		
		$output = '(' . $relq->build_sql_query_select(). ')';
		
		return $output;
	}
	
	/**
	 * Builds a group function for the SQL select statement (e.g. "SUM(field)") from an ExFace aggregator
	 * function. This method translates ExFace aggregators to SQL und thus will probably differ between
	 * SQL dialects.
	 * TODO Currently this method also parses the ExFace aggregator. This probably should be moved to the
	 * \exface\Core\CommonLogic\QueryBuilder\QueryPart because it is something entirely ExFace-specific an does not depend on the data source. It
	 * would also make it easier to override this method for specific sql dialects while reusing some
	 * basics (like SUM or AVG) from the general sql query builder.
	 * 
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
			case 'COUNT_DISTINCT':
				$output = "COUNT(DISTINCT " . $select . ")";
				break;
			default:
				break;
		}
		return $output;
	}
	
	protected function build_sql_from (){
		// here we simply have to replace the placeholders in case the from-clause ist a custom sql statement
		return str_replace('[#alias#]', $this->get_main_object()->get_alias(), $this->get_main_object()->get_data_address()) . ' ' . $this->get_main_object()->get_alias() . $this->get_query_id();
	}
	
	/**
	 * 
	 * @param \exface\Core\CommonLogic\QueryBuilder\QueryPart $qpart
	 * @param string $left_table_alias
	 * @return array [ relation_path_relative_to_main_object => join_string ]
	 */
	protected function build_sql_joins (\exface\Core\CommonLogic\QueryBuilder\QueryPart $qpart, $left_table_alias = ''){
		$joins = array();
		
		if ($qpart instanceof QueryPartFilterGroup){
			// This extra if for the filter groups is a somewhat strange solutions for reverse relation filters being ignored within groups. It seems, that
			// if you use $qpart->get_used_relations() on a FilterGroup and just continue with the "else" part of this if, reverse relations are being ignored.
			// The problem is, that the special treatment for attributes of the main object and an explicit left_table_alias should be applied to filter group
			// at some point, but it is not, because it is not possible to determine, what object the filter group belongs to (it might have attributes from
			// many object). I don not understand, however, why that special treatment seems to be important for reverse relations... In any case, this recursion
			// does the job.
			foreach ($qpart->get_filters_and_nested_groups() as $f){
				$joins = array_merge($joins, $this->build_sql_joins($f));
			}
		} else {
			$rels = $qpart->get_used_relations();
			
			if (count($rels) === 0 
			&& $qpart->get_attribute()->get_object_id() == $this->get_main_object()->get_id() 
			&& $left_table_alias){
				// Special treatment if we are joining attributes of the main object to an explicitly specified table alias.
				// This is necessary when putting some special attributes of the main object (i.e. those with custom
				// sql) into the enrichment query. In this case, we need to join the table of the main object to
				// the core query again, after pagination, so possible back references within the custom select can
				// still be resolved.
				$right_table_alias = $this->get_short_alias($this->get_main_object()->get_alias()) . $this->get_query_id();
				$joins[$right_table_alias] = "\n LEFT JOIN " . str_replace('[#alias#]', $right_table_alias, $this->get_main_object()->get_data_address()) . ' ' . $right_table_alias . ' ON ' . $left_table_alias . '.' . $this->get_main_object()->get_uid_alias() . ' = ' . $right_table_alias . '.' . $this->get_main_object()->get_uid_alias();
			} else {
				// In most cases we will build joins for attributes of related objects.
				$left_table_alias = $this->get_short_alias($left_table_alias ? $left_table_alias : $this->get_main_object()->get_alias()) . $this->get_query_id();
				$left_obj = $this->get_main_object();
				foreach ($rels as $alias => $rel){
					if ($rel->get_type() == 'n1'){
						$right_table_alias = $this->get_short_alias($alias) . $this->get_query_id();
						$right_obj = $this->get_main_object()->get_related_object($alias);
						// generate the join sql
						$joins[$right_table_alias] = "\n " . $rel->get_join_type() . ' JOIN ' . str_replace('[#alias#]', $right_table_alias, $right_obj->get_data_address()) . ' ' . $right_table_alias . ' ON ' . $left_table_alias . '.' . $left_obj->get_attribute($rel->get_foreign_key_alias())->get_data_address() . ' = ' . $right_table_alias . '.' . $rel->get_related_object_key_attribute()->get_data_address();
						// continue with the related object
						$left_table_alias = $right_table_alias;
						$left_obj = $right_obj;
					} elseif ($rel->get_type() == '11'){
						// TODO 1-to-1 relations
					} else {
						// stop joining as all the following joins will be add in subselects of the enrichment select
						break;
					}
				}
			}
		}
		return $joins;
	}
	
	/**
	 * Builds a where statement for a group of filters, concatennating the conditions with the goups logical operator
	 * (e.g. " condition1 AND condition 2 AND (condition3 OR condition4) ")
	 * @param QueryPartFilterGroup $qpart
	 * @return string
	 */
	protected function build_sql_where(QueryPartFilterGroup $qpart, $rely_on_joins = true){
		$where = '';
		
		switch ($qpart->get_operator()){
			case EXF_LOGICAL_AND : $op = 'AND'; break;
			case EXF_LOGICAL_OR : $op = 'OR'; break;
			case EXF_LOGICAL_XOR : $op = 'XOR'; break;
			case EXF_LOGICAL_NOT : $op = 'NOT'; break;
		}
		
		foreach ($qpart->get_filters() as $qpart_fltr){
			if ($fltr_string = $this->build_sql_where_condition($qpart_fltr, $rely_on_joins)){
				$where .= "\n " . ($where ? $op . " " : '') . $fltr_string;
			}
		}
		
		foreach ($qpart->get_nested_groups() as $qpart_grp){
			if ($grp_string = $this->build_sql_where($qpart_grp, $rely_on_joins)){
				$where .= "\n " . ($where ? $op . " " : '') . "(" . $grp_string . ")";
			}
		}
		
		return $where;
	}
	
	/**
	 * Builds a single filter condition for the where clause (e.g. " table.column LIKE '%string%' ")
	 * @param \exface\Core\CommonLogic\QueryBuilder\QueryPartFilter $qpart
	 * @return boolean|string
	 */
	protected function build_sql_where_condition (\exface\Core\CommonLogic\QueryBuilder\QueryPartFilter $qpart, $rely_on_joins = true){
		$val = $qpart->get_compare_value();
		$attr = $qpart->get_attribute();
		$comp = $qpart->get_comparator();
		
		// always use the equals comparator for foreign keys! It's faster!
		if ($attr->is_relation() && $comp != EXF_COMPARATOR_IN){
			$comp = EXF_COMPARATOR_EQUALS;
		} elseif ($attr->get_alias() == $this->get_main_object()->get_uid_alias() && $comp != EXF_COMPARATOR_IN) {
			$comp = EXF_COMPARATOR_EQUALS;
		}
		// also use equals for the NUMBER data type, but make sure, the value to compare to is really a number (otherwise the query will fail!)
		elseif ($attr->get_data_type()->is(EXF_DATA_TYPE_NUMBER) && $comp == EXF_COMPARATOR_IS && is_numeric($val)) {
			$comp = EXF_COMPARATOR_EQUALS;
		}
		// also use equals for the BOOLEAN data type
		elseif ($attr->get_data_type()->is(EXF_DATA_TYPE_BOOLEAN) && $comp == EXF_COMPARATOR_IS) {
			$comp = EXF_COMPARATOR_EQUALS;
			$val = $this->prepare_input_value($val, $attr->get_data_type());
		}

		$select = $attr->get_data_address();
		$where = $qpart->get_data_address_property('WHERE');
		$object_alias = ($attr->get_relation_path()->to_string() ? $attr->get_relation_path()->to_string() : $this->get_main_object()->get_alias());
	
		// doublecheck that the attribut is known
		if (!($select || $where) || $val === ''){
			throw new QueryBuilderException('Illegal filter on object "' . $this->get_main_object()->get_alias() . ', expression "' . $qpart->get_alias() . '", Value: "' . $val . '".');
			return false;
		}
		
		if ($qpart->get_first_relation('1n') || ($rely_on_joins == false && count($qpart->get_used_relations()) > 0)){
			// Use subqueries for attributes with reverse relations and in case we know, tha main query will not have any joins (e.g. UPDATE queries)
			$output = $this->build_sql_where_subquery($qpart, $rely_on_joins);
		} else {
			// build the where
			if ($where){
				// check if it has an explicit where clause. If not try to filter based on the select clause
				$output = str_replace(array('[#alias#]', '[#value#]'), array($object_alias . $this->get_query_id(), $val), $where);
			} else {
				// Determine, what we are going to compare to the value: a subquery or a column
				if ($this->check_for_sql_statement($attr->get_data_address())){
					$subj = str_replace(array('[#alias#]'), array($this->get_short_alias($object_alias) . $this->get_query_id()), $select);
				} else {
					$subj = $this->get_short_alias($object_alias) . $this->get_query_id() . '.' . $select;
				}
				// Do the actual comparing
				$output = $this->build_sql_where_comparator($subj, $comp, $val, $attr->get_data_type(), $attr->get_data_address_property('SQL_DATA_TYPE'));
			}
		}
		return $output;
	}
	
	protected function build_sql_where_comparator($subject, $comparator, $value, AbstractDataType $data_type, $sql_data_type = NULL){
		// Check if the value is of valid type. 
		try {
			// Pay attention to comparators expecting concatennated values (like IN) - the concatennated value will not validate against
			// the data type, but the separated parts should
			if ($comparator != EXF_COMPARATOR_IN){
				$value = $data_type::parse($value);
			} else {
				$values = explode(',', $value);
				foreach ($values as $nr => $val){
					$values[$nr] = $data_type::parse($val);
				}
				$value = implode(',', $values);
			}
		} catch (DataTypeValidationError $e) {
			// TODO Not sure, if it is wise to skip invalid filters. Perhaps we should rethrow the exception here. This would, howerver
			// cause error on bad prefills, etc. Maybe throw a warning, once the separation of errors and warnings is implemented
			$e->rethrow();
			return '';
		}
		
		// If everything is OK, build the SQL
		switch ($comparator){
			case EXF_COMPARATOR_IN: $output = $subject . " IN (" . $value . ")"; break;
			case EXF_COMPARATOR_EQUALS: $output = $subject . " = " . $this->prepare_where_value($value, $data_type, $sql_data_type); break;
			case EXF_COMPARATOR_EQUALS_NOT: $output = $subject . " != " . $this->prepare_where_value($value, $data_type, $sql_data_type); break;
			case EXF_COMPARATOR_GREATER_THAN:
			case EXF_COMPARATOR_LESS_THAN:
			case EXF_COMPARATOR_GREATER_THAN_OR_EQUALS:
			case EXF_COMPARATOR_LESS_THAN_OR_EQUALS: $output = $subject . " " . $comparator . " " . $this->prepare_where_value($value, $data_type, $sql_data_type); break;
			case EXF_COMPARATOR_IS_NOT: $output = 'UPPER(' . $subject . ") NOT LIKE '%" . strtoupper($value) . "%'";
			case EXF_COMPARATOR_IS:
			default: $output = 'UPPER(' . $subject . ") LIKE '%" . strtoupper($value) . "%'";
		}
		return $output;
	}
	
	protected function prepare_where_value($value, AbstractDataType $data_type, $sql_data_type = NULL){
		// IDEA some data type specific procession here
		$output = $value;
		return $output;
	}
	
	/**
	 * Builds a WHERE clause with a subquery (e.g. "column IN ( SELECT ... )" ). This is mainly used to handle filters over reversed relations, but also
	 * for filters on joined columns in UPDATE queries, where the main query does not support joining. The optional parameter $rely_on_joins controls whether 
	 * the method can rely on the main query have all neccessary joins.
	 * @param \exface\Core\CommonLogic\QueryBuilder\QueryPartFilter $qpart
	 * @param boolean $rely_on_joins
	 */
	protected function build_sql_where_subquery(QueryPartFilter $qpart, $rely_on_joins = true){
		/* @var $start_rel \exface\Core\CommonLogic\Model\relation */
		// First of all, see if we can rely on all joins being performed in the main query.
		// This is implicitly also the case, if there are no joins needed (= the data in the main query will be sufficient in any case)
		if($rely_on_joins || count($qpart->get_used_relations()) === 0){
			// If so, just need to include those relations in the subquery, which follow a reverse relation
			$start_rel = $qpart->get_first_relation('1n');
		} else {
			// Otherwise, all relations (starting from the first one) must be put into the subquery, because there are no joins in the main one
			$start_rel = $qpart->get_first_relation();
		}
		
		if ($start_rel){
			/** @var string part of the relation part up to the first reverse relation */
			$prefix_rel_path = RelationPath::relaton_path_cut($qpart->get_alias(), null, $start_rel->get_alias());
			/** @var string complete path of the first reverse relation */
			$start_rel_path = RelationPath::relation_path_add($prefix_rel_path, $start_rel->get_alias());
		
			// build a subquery
			/* @var $relq \exface\SqlDataConnector\QueryBuilders\AbstractSQL */
			$qb_class = get_class($this);
			$relq = new $qb_class;
			$relq->set_main_object($start_rel->get_related_object());
			$relq->set_query_id($this->get_next_subquery_id());
			if ($start_rel->get_type() == '1n'){
				// If we are dealing with a reverse relation, build a subquery to select foreign keys from rows of the joined tables, tha match the given filter
				$rel_filter = RelationPath::relaton_path_cut($qpart->get_attribute()->get_alias_with_relation_path(), $start_rel->get_alias());
				$relq->add_attribute($start_rel->get_foreign_key_alias());
				// Add the filter relative to the first reverse relation with the same $value and $comparator
				$relq->add_filter_from_string($rel_filter, $qpart->get_compare_value() , $qpart->get_comparator());
				// FIXME add support for related_object_special_key_alias
				if ($prefix_rel_path){
					$prefix_rel_qpart = new \exface\Core\CommonLogic\QueryBuilder\QueryPartSelect(RelationPath::relation_path_add($prefix_rel_path, $this->get_main_object()->get_related_object($prefix_rel_path)->get_uid_alias()), $this);
					$junction = $this->build_sql_select($prefix_rel_qpart, null, null, '');
				} else {
					$junction = $this->get_short_alias($this->get_main_object()->get_alias() . $this->get_query_id()) . '.' . $this->get_main_object()->get_uid_attribute()->get_data_address();
				}
			} else {
				// If we are dealing with a regular relation, build a subquery to select primary keys from joined tables and match them to the foreign key of the main table
				$relq->add_filter($qpart->rebase($relq, $start_rel->get_alias()));
				$relq->add_attribute($start_rel->get_related_object_key_alias());
				$junction_qpart = new \exface\Core\CommonLogic\QueryBuilder\QueryPartSelect($start_rel->get_foreign_key_alias(), $this);
				$junction = $this->build_sql_select($junction_qpart, null, null, ''); 
			}
			
			$output = $junction . ' IN (' . $relq->build_sql_query_select(). ')';
		}
		
		return $output;
	}
	
	/**
	 * Builds the contents of an ORDER BY statement for one column (e.g. "ATTRIBUTE_ALIAS DESC" to sort via
	 * the column ALIAS of the table ATTRIBUTE). The result does not contain the words "ORDER BY", the 
	 * results of multiple calls to this method with different attributes can be concatennated into 
	 * a comple ORDER BY clause.
	 * @param \exface\Core\CommonLogic\QueryBuilder\QueryPartSorter $qpart
	 * @return string
	 */
	protected function build_sql_order_by(\exface\Core\CommonLogic\QueryBuilder\QueryPartSorter $qpart){
		if ($qpart->get_data_address_property("ORDER_BY")){
			$output = $this->get_short_alias($this->get_main_object()->get_alias()) . '.' . $qpart->get_data_address_property("ORDER_BY");
		} else {
			$output = $this->get_short_alias($qpart->get_alias());
		}
		$output .= ' ' . $qpart->get_order();
		return $output;
	}
	
	/**
	 * Builds the contents of an GROUP BY statement for one column (e.g. "ATTRIBUTE.ALIAS" to group by the
	 * the column ALIAS of the table ATTRIBUTE). The result does not contain the words "GROUP BY", thus 
	 * the results of multiple calls to this method with different attributes can be concatennated into 
	 * a comple GROUP BY clause.
	 * @param \exface\Core\CommonLogic\QueryBuilder\QueryPartSorter $qpart
	 * @param string $select_from
	 * @return string
	 */
	protected function build_sql_group_by(\exface\Core\CommonLogic\QueryBuilder\QueryPart $qpart, $select_from = null){
		$output = '';
		if ($this->check_for_sql_statement($qpart->get_attribute()->get_data_address())){
			// Seems like SQL statements are not supported in the GROUP BY clause in general
			throw new QueryBuilderException('Cannot use the attribute "' . $qpart->get_attribute()->get_alias_with_relation_path() . '" for aggregation in an SQL data source, because it\'s data address is defined via custom SQL statement');
		} else {
			// If it's not a custom SQL statement, it must be a column, so we need to prefix it with the table alias
			if (is_null($select_from)){
				$select_from = $qpart->get_attribute()->get_relation_path()->to_string() ? $qpart->get_attribute()->get_relation_path()->to_string() : $this->get_main_object()->get_alias(); 
			}
			$output = ($select_from ? $this->get_short_alias($select_from) . '.' : '') . $this->get_short_alias($qpart->get_attribute()->get_data_address());
		}
		return $output;
	}
	
	/**
	 * Shortens an alias (or any string) to $short_alias_max_length by cutting off the rest and appending
	 * a unique id. Also replaces forbidden words and characters ($short_alias_forbidden and $short_alias_remove_chars).
	 * The result can be translated back to the original via get_full_alias($short_alias)
	 * Every SQL-alias (like "SELECT xxx AS alias" or "SELECT * FROM table1 alias") should be shortened
	 * because most SQL dialects only allow a limited number of characters in an alias (this number should
	 * be set in $short_alias_max_length).
	 * @param unknown $full_alias
	 * @return Ambigous <string, unknown, multitype:>
	 */
	protected function get_short_alias($full_alias){
		if (isset($this->short_aliases[$full_alias])){
			$short_alias = $this->short_aliases[$full_alias];
		} elseif (strlen($full_alias) <= $this->short_alias_max_length 
				&& $this->get_clean_alias($full_alias) == $full_alias
				&& !in_array($full_alias, $this->short_alias_forbidden)){
			$short_alias = $full_alias;
		} else {
			$this->short_alias_index++;
			$short_alias = substr($this->get_clean_alias($full_alias), 0, ($this->short_alias_max_length - strlen($this->short_alias_index))) . $this->short_alias_index;
			$this->short_aliases[$full_alias] = $short_alias;
		}
		
		return $short_alias;
	}
	
	protected function get_clean_alias($alias){
		$output = '';
		$output = str_replace($this->short_alias_remove_chars, '_', $alias);
		return $output;
	}
	
	protected function get_full_alias($short_alias){
		$full_alias = array_search($short_alias, $this->short_aliases);
		if ($full_alias === false){
			$full_alias = $short_alias;
		}
		return $full_alias;
	}
	
	/**
	 * Checks, if the given string is complex SQL-statement (in contrast to simple column references). It is
	 * important to know this, because you cannot write to statements etc.
	 * @param string string
	 * @return boolean
	 */
	protected function check_for_sql_statement($string){
		if (strpos($string, '(') !== false && strpos($string, ')') !== false) return true;
		return false;
	}
	
	protected function add_binary_column($full_alias){
		$this->binary_columns[] = $full_alias;
		return $this;
	}
	
	protected function get_binary_columns(){
		return $this->binary_columns;
	}
	
	protected function decode_binary($value){
		$hex_value = bin2hex($value);
		return ($hex_value ? '0x' : '') . $hex_value;
	}
	
	/**
	 * Returns an array with attributes to be joined over reverse relations (similarly to get_attributes(), which returns all attributes)
	 * @return QueryPartAttribute[]
	 */
	protected function get_attributes_with_reverse_relations(){
		$result = array();
		foreach ($this->get_attributes() as $alias => $qpart){
			if ($qpart->get_used_relations('1n')){
				$result[$alias] = $qpart;
			}
		}
		return $result;
	}
	
	public function get_query_id() {
		return $this->query_id;
	}
	
	public function set_query_id($value) {
		$this->query_id = $value;
		return $this;
	}
	
	protected function get_next_subquery_id(){
		return ++$this->subquery_counter;
	}
}
?>