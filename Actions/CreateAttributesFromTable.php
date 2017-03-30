<?php namespace exface\SqlDataConnector\Actions;

use exface\Core\CommonLogic\AbstractAction;
use exface\Core\Exceptions\Actions\ActionInputInvalidObjectError;
use exface\SqlDataConnector\Interfaces\SqlDataConnectorInterface;
use exface\Core\Exceptions\Actions\ActionInputTypeError;

/**
 * This action runs one or more selected test steps
 * 
 * @author Andrej Kabachnik
 *
 */
class CreateAttributesFromTable extends AbstractAction {
	
	protected function init(){
		$this->set_icon_name('gears');
		$this->set_input_rows_min(1);
		$this->set_input_rows_max(null);
	}	
	
	protected function perform(){
		if (!$this->get_input_data_sheet()->get_meta_object()->is('exface.Core.OBJECT')){
			throw new ActionInputInvalidObjectError($this, 'Action "' . $this->get_alias() . '" exprects an exface.Core.OBJECT as input, "' . $this->get_input_data_sheet()->get_meta_object()->get_alias_with_namespace() . '" given instead!');
		}
		
		$result_data_sheet = $this->get_workbench()->data()->create_data_sheet($this->get_workbench()->model()->get_object('exface.Core.ATTRIBUTE'));
		$skipped_columns = 0;
		foreach ($this->get_input_data_sheet()->get_rows() as $input_row){
			/* @var $target_obj \exface\Core\CommonLogic\Model\Object */
			$target_obj = $this->get_workbench()->model()->get_object($input_row[$this->get_input_data_sheet()->get_uid_column()->get_name()]);
			$target_obj_connection = $target_obj->get_data_connection();
			if (!($target_obj_connection instanceof SqlDataConnectorInterface)){
				throw new ActionInputTypeError($this, 'Cannot create attributes from SQL table for data connection "' . $target_obj_connection->get_alias_with_namespace() . '": only SQL connections supported (must implement the SqlDataConnectorInterface!).');
			}
			foreach ($target_obj_connection->get_sql_explorer()->get_attribute_properties_from_table($target_obj, $input_row['DATA_ADDRESS']) as $row){
				if ($target_obj->find_attributes_by_data_address($row['DATA_ADDRESS'])){
					$skipped_columns++;
					continue;
				}
				$result_data_sheet->add_row($row);
			}
		}
		
		if (!$result_data_sheet->is_empty()){
			$result_data_sheet->data_create();
		}
			
		// Save the result and output a message for the user
		$this->set_result_data_sheet($result_data_sheet);
		$this->set_result('');
		$this->set_result_message('Created ' . $result_data_sheet->count_rows() . ' attribute(s) for ' . $this->get_input_data_sheet()->count_rows() . ' object(s). ' . $skipped_columns . ' attributes skipped as duplicates!');
		
		return;
	}
}
?>