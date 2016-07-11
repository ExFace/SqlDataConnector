<?php namespace exface\SqlDataConnector\ModelLoaders;

use exface\Core\Interfaces\DataSources\ModelLoaderInterface;
use exface\Core\Exceptions\MetaModelObjectNotFoundException;
use exface\Core\CommonLogic\Model\Attribute;
use exface\Core\CommonLogic\Model\Relation;
use exface\Core\CommonLogic\Model\Object;
use exface\Core\CommonLogic\UxonObject;
use exface\Core\Factories\DataSorterFactory;
use exface\Core\Interfaces\DataSources\DataConnectionInterface;
use exface\Core\Exceptions\DataSourceError;
use exface\Core\Interfaces\DataSources\DataSourceInterface;
use exface\Core\Factories\ConditionFactory;
use exface\SqlDataConnector\Interfaces\SqlDataConnectorInterface;
use exface\Core\Exceptions\ModelLoaderError;
use exface\Core\Factories\BehaviorFactory;

class SqlModelLoader implements ModelLoaderInterface {
	private $data_connection = null;
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\Core\Interfaces\DataSources\ModelLoaderInterface::load_object()
	 */
	public function load_object(Object $object){
		$exface = $object->exface();
		$load_behaviors = false;
		if ($object->get_id()){
			$q_where = 'o.oid = ' . $object->get_id();
		} else {
			$q_where = 'a.app_alias = "' . $object->get_namespace() . '" AND o.object_alias = "' . $object->get_alias() . '"';
		}
		$res = $this->get_data_connection()->query('
				SELECT
					' . $this->generate_sql_uuid_selector('o.oid') . ' as oid,
					' . $this->generate_sql_uuid_selector('o.app_oid') . ' as app_oid,
					a.app_alias,
					o.object_name,
					o.object_alias,
					o.data_address,
					o.data_address_properties,
					' . $this->generate_sql_uuid_selector('o.data_source_oid') . ' as data_source_oid,
					' . $this->generate_sql_uuid_selector('o.parent_object_oid') . ' as parent_object_oid,
					o.short_description,
					o.long_description,
					o.default_editor_uxon,
					' . $this->generate_sql_uuid_selector('ds.base_object_oid') . ' as base_object_oid,
					EXISTS (SELECT 1 FROM exf_object_behaviors ob WHERE ob.object_oid = o.oid) AS has_behaviors
				FROM exf_object o 
					LEFT JOIN exf_app a ON o.app_oid = a.oid 
					LEFT JOIN exf_data_source ds ON o.data_source_oid = ds.oid
				WHERE ' . $q_where);
		if ($res){
			$row = $res[0];
			
			$object->set_id($row['oid']);
			$object->set_name($row['object_name']);
			$object->set_data_address($row['data_address']);
			$object->set_data_address_properties(UxonObject::from_json($row['data_address_properties']));
			$object->set_alias($row['object_alias']);
			$object->set_data_source_id($row['data_source_oid']);
			$object->set_app_id($row['app_oid']);
			$object->set_namespace($row['app_alias']);
			$object->set_short_description($row['short_description']);
			$object->set_default_editor_uxon(UxonObject::from_json($row['default_editor_uxon']));
			if ($row['has_behaviors']) {
				$load_behaviors = true;
			}
				
			// find all parents
			// When loading a data source base object, make sure not to inherit from itself to avoid recursion.
			if ($row['base_object_oid'] && $row['base_object_oid'] != $object->get_id()) {
				$object->extend_from_object_id($row['base_object_oid']);
			}
			if ($row['parent_object_oid']) {
				$object->extend_from_object_id($row['parent_object_oid']);
			}
		} else {
			throw new MetaModelObjectNotFoundException('Object with alias "' . $object->get_alias_with_namespace() . '" or id "' . $object->get_id() . '" not found!');
		}
		
		// select all attributes for this object
		$res = $this->get_data_connection()->query('
				SELECT
					a.*,
					' . $this->generate_sql_uuid_selector('a.oid') . ' as oid,
					' . $this->generate_sql_uuid_selector('a.object_oid') . ' as object_oid,
					' . $this->generate_sql_uuid_selector('a.related_object_oid') . ' as related_object_oid,
					' . $this->generate_sql_uuid_selector('a.related_object_special_key_attribute_oid') . ' as related_object_special_key_attribute_oid,
					d.data_type_alias,
					d.default_widget_uxon AS default_data_type_editor,
					o.object_alias as rev_relation_alias,
					o.object_name AS rev_relation_name
				FROM exf_attribute a LEFT JOIN exf_object o ON a.object_oid = o.oid LEFT JOIN exf_data_type d ON d.oid = a.data_type_oid
				WHERE a.object_oid = ' . $object->get_id() . ' OR a.related_object_oid = ' . $object->get_id());
		if($res){
			// use a for here instead of foreach because we want to extend the array from within the loop on some occasions
			$l = count($res);
			for ($i = 0; $i < $l; $i++){
				$row = $res[$i];
				// Only create attributes, that really belong to this object. Inherited attributes are already there.
				if ($row['object_oid'] == $object->get_id()){
					// save the label attribute alias in object head
					if ($row['object_label_flag']){
						$object->set_label_alias($row['attribute_alias']);
						// always add a LABEL attribute if it is not already called LABEL (widgets always need to show the LABEL!)
						// IDEA cleaner code does not work for some reason. Didn't have time to check out why...
						/*if ($row['attribute_alias'] != $object->get_model()->exface()->get_config_value('object_label_alias')){
						 $label_attribute = attribute::from_db_row($row);
						 $label_attribute->set_alias($object->get_model()->exface()->get_config_value('object_label_alias'));
						 $label_attribute->set_default_display_order(-1);
						 $object->get_attributes()->add($label_attribute);
						 }*/
						if ($row['attribute_alias'] != $object->get_model()->exface()->get_config_value('object_label_alias')){
							$label_attribute = $row;
							$label_attribute['attribute_alias'] = $object->get_model()->exface()->get_config_value('object_label_alias');
							$label_attribute['attribute_hidden_flag'] = '1';
							$label_attribute['attribute_required_flag'] = '0';
							// The special label attribute should not be marked as label because it then would be returned by get_label..(),
							// which instead should return the original attribute
							$label_attribute['object_label_flag'] = 0;
							// If label and UID are one attribute, make sure the special LABEL attribute will not be treated as a second UID!
							$label_attribute['object_uid_flag'] = '0';
							unset($label_attribute['default_display_order']);
							$res[] = $label_attribute;
							$l++;
						}
					}
						
					// check if an attribute is marked as unique id for this object
					if ($row['object_uid_flag']){
						$object->set_uid_alias($row['attribute_alias']);
						$row['system_flag'] = true;
					}
						
					// check if the attribute is part of the default sorting
					if ($row['default_sorter_order']){
						$sorter = DataSorterFactory::create_empty($exface);
						$sorter->set_attribute_alias($row['attribute_alias']);
						$sorter->set_direction($row['default_sorter_dir']);
						$object->get_default_sorters()->add($sorter, $row['default_sorter_order']);
					}
					
					// populate attributes
					$attr = $this->create_attribute_from_db_row($object, $row);
					// Add the attribute to the object giving the alias as key explicitly, because automatic key generation will
					// fail here in an infinite loop, because it uses get_relation_path(), etc.
					// TODO Check if get_alias_with_relation_path() really will cause loops inevitably. If not, remove the explicit key
					// here.
					$object->get_attributes()->add($attr, $attr->get_alias());
				}
					
				// Now populate relations, if the attribute is a relation. This is done for own attributes as well as inherited ones because
				// the latter may be involved in reverse relations. But this means also, that direct relations can only be created from direct
				// attributes.
				if ($row['related_object_oid']){
					// we have a reverse (1-n) relation if the attribute belongs to another object and that object is not being extended from
					// Otherwise it's a normal n-1 relation
					// IDEA What if we also create relations between parent and child objects. The inheriting object should probably get a direct
					// relation to the parent. Would that be usefull for objects sharing attributes but using different data_addresses?
					if ($object->get_id() != $row['object_oid'] && !in_array($row['object_oid'], $object->get_parent_objects_ids())){
						// FIXME what is the related_object_key_alias for reverse relations?
						$rel = new Relation(
								$row['oid'], // id
								$row['rev_relation_alias'], // alias
								$row['rev_relation_name'], // name (used for captions)
								$row['related_object_oid'], // main object
								$row['attribute_alias'], // foreign key in the main object
								$row['object_oid'], // related object
								null, // related object key attribute (uid)
								'1n'); // relation type
					} elseif ($attr) {
						// At this point, we know, it is a direct relation. This can only happen if the object has a corresponding direct
						// attribute. This is why the elseif($attr) is there.
						$rel = new Relation(
								$attr->get_id(),
								$attr->get_alias(),
								$attr->get_name(),
								$object->get_id(),
								$attr->get_alias(),
								$row['related_object_oid'],
								$row['related_object_special_key_attribute_oid'],
								'n1');
					}
						
					if ($rel){
						$object->add_relation($rel);
					}
				}
			}
		}
		
		// Load behaviors if needed
		if ($load_behaviors){
			$res = $this->get_data_connection()->query('
				SELECT * FROM exf_object_behaviors WHERE object_oid = ' . $object->get_id()
			);
			if ($res){
				foreach ($res as $row){
					$behavior = BehaviorFactory::create_from_uxon($object, $row['behavior'], UxonObject::from_json($row['config_uxon']));
					$object->get_behaviors()->add($behavior);
				}
			}
		}
		
		return $object;
	}
	
	protected function create_attribute_from_db_row(Object &$object, array $row){
		$model = $object->get_model();
		$attr = new Attribute($model);
		// ensure the attributes all have the correct parent object (because inherited attributes actually would 
		// have another object_id in their row data)
		$attr->set_object_id($object->get_id());
		$attr->set_id($row['oid']);
		$attr->set_alias($row['attribute_alias']);
		$attr->set_name($row['attribute_name']);
		$attr->set_data_address($row['data']);
		$attr->set_data_address_properties(UxonObject::from_json($row['data_properties']));
		$attr->set_formatter($row['attribute_formatter']);
		$attr->set_data_type($row['data_type_alias']);
		$attr->set_required($row['attribute_required_flag']);
		$attr->set_editable($row['attribute_editable_flag']);
		$attr->set_hidden($row['attribute_hidden_flag']);
		$attr->set_system($row['system_flag']);
		$attr->set_default_display_order($row['default_display_order']);
		$attr->set_relation_flag($row['related_object_oid'] ? true : false);
		$attr->set_default_value($row['default_value']);
		$attr->set_fixed_value($row['fixed_value']);
		$attr->set_formula($row['attribute_formula']);
		$attr->set_default_sorter_dir($row['default_sorter_dir']);
		$attr->set_short_description($row['attribute_short_description']);
	
		// Create the UXON for the default editor widget
		// Start with the data type widget
		$uxon = UxonObject::from_json($row['default_data_type_editor']);
		// If anything goes wrong, create a blank widget with the overall default widget type (from the config)
		if (!$uxon) {
			$uxon = new UxonObject();
			$uxon->set_property('widget_type', $model->exface()->get_config_value('widget_for_unknown_data_types'));
		}
		// Add some attribute specific values
		$uxon->caption = $attr->get_name();
		$uxon->hint = $attr->get_hint();
		// Extend by the specific uxon for this attribute if specified
		if ($row['default_editor_uxon']){
			$uxon = $uxon->extend(UxonObject::from_json($row['default_editor_uxon']));
		}
		$attr->set_default_widget_uxon($uxon);
	
		return $attr;
	}
	
	public function load_data_source(DataSourceInterface $data_source, $data_connection_id_or_alias = NULL){
		// If the data connector was not set for this data source previously, load it now
		if (!$data_source->get_data_connector_alias()){
			if ($data_connection_id_or_alias){
				// See if a (hex-)ID is given or an alias. The latter will need to be wrapped in qotes!
				if (strpos($data_connection_id_or_alias, '0x') !== 0){
					$data_connection_id_or_alias = '"' . $data_connection_id_or_alias . '"';
				}
				$join_on = "(dc.oid = " . $data_connection_id_or_alias . " OR dc.alias = " . $data_connection_id_or_alias . ")";
			} else {
				$join_on = 'ds.data_connection_oid = dc.oid';
			}
			
			// If there is a user logged in, fetch his specific connctor config (credentials)
			if ($user_name = $data_source->exface()->context()->get_scope_user()->get_user_name()){
				$join_user_credentials = ' LEFT JOIN (exf_data_connection_credentials dcc LEFT JOIN exf_user_credentials uc ON dcc.user_credentials_oid = uc.oid INNER JOIN exf_user u ON uc.user_oid = u.oid AND u.username = "' . $user_name . '") ON dcc.data_connection_oid = dc.oid';
				$select_user_credentials = ', uc.data_connector_config AS user_connector_config';
			}
			
			$query = '
				SELECT 
					ds.default_query_builder, 
					ds.read_only_flag AS data_source_read_only, 
					dc.read_only_flag AS connection_read_only, 
					CONCAT(\'0x\', HEX(dc.oid)) AS data_connection_oid, 
					dc.name, 
					dc.data_connector, 
					dc.data_connector_config, 
					dc.filter_context_uxon' . $select_user_credentials . ' 
				FROM exf_data_source ds LEFT JOIN exf_data_connection dc ON ' . $join_on . $join_user_credentials . ' 
				WHERE ds.oid = ' . $data_source->get_id();
			$ds = $this->get_data_connection()->query($query);
			if (count($ds) > 1){
				throw new DataSourceError('Multiple user credentials found for data connection "'. $data_connection_id_or_alias . '" and user "' . $user_name . '"!');
			} elseif (count($ds) != 1){
				throw new DataSourceError('Cannot find data connection "'. $data_connection_id_or_alias . '"!');
			}
			$ds = $ds[0];
			$data_source->set_data_connector_alias($ds['data_connector']);
			$data_source->set_connection_id($ds['data_connection_oid']);
			$data_source->set_read_only(($ds['data_source_read_only'] || $ds['connection_read_only']) ? true : false);
			// Some data connections may have their own filter context. Add them to the application context scope
			if ($ds['filter_context_uxon'] && $filter_context = json_decode($ds['filter_context_uxon'])){
				if (!is_array($filter_context)){
					$filter_context = array($filter_context);
				}
				foreach ($filter_context as $filter){
					$condition = ConditionFactory::create_from_object_or_array($data_source->exface(), $filter);
					$data_source->exface()->context()->get_scope_application()->get_filter_context()->add_condition($condition);
				}
			}
		}
		
		// The query builder: if not given, use the default one from the data source configuration
		if (!$data_source->get_query_builder_alias()){
			$data_source->set_query_builder_alias($ds['default_query_builder']);
		}
		
		// The configuration of the connection: if not given, get the configuration from DB
		$data_source->set_connection_id($ds['data_connection_oid']);
		$config = UxonObject::from_json($ds['data_connector_config']);
		$config = $config->extend(UxonObject::from_json($ds['user_connector_config']));
		if (is_object($config)){
			$config = (array) $config;
		}
		$data_source->set_connection_config($config);
		
		return $data_source;
	}
	
	/**
	 * Ensures that binary UID fields are selected as 0xNNNNN to be compatible with the internal binary notation in ExFace
	 * @param string $field_name
	 * @return string
	 */
	protected function generate_sql_uuid_selector($field_name){
		return 'CONCAT(\'0x\', HEX(' . $field_name . '))';
	}
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\Core\Interfaces\DataSources\ModelLoaderInterface::get_data_connection()
	 */
	public function get_data_connection() {
		return $this->data_connection;
	}
	
	/**
	 * 
	 * {@inheritDoc}
	 * @see \exface\Core\Interfaces\DataSources\ModelLoaderInterface::set_data_connection()
	 */
	public function set_data_connection(DataConnectionInterface &$connection) {
		if (!($connection instanceof SqlDataConnectorInterface)){
			throw new ModelLoaderError('Cannot use data connection "' . get_class($connection) . '" for the SQL model loader: the connection must implement the SqlDataConnector interface!');
		}
		$this->data_connection = $connection;
		return $this;
	}
}

?>