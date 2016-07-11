<?php namespace exface\SqlDataConnector\Actions;

use exface\Core\Exceptions\ActionRuntimeException;
use exface\Core\CommonLogic\AbstractAction;

/**
 * This action runs one or more selected test steps
 * 
 * @author aka
 *
 */
class CreateAttributesFromTable extends AbstractAction {
	
	protected function init(){
		$this->set_icon_name('gears');
		$this->set_input_rows_min(1);
		$this->set_input_rows_max(null);
	}	
	
	protected function perform(){
		if (strcasecmp($this->get_input_data_sheet()->get_meta_object()->get_alias_with_namespace(), 'exface.Core.OBJECT') != 0){
			throw new ActionRuntimeException('Action "' . $this->get_alias() . '" exprects an exface.Core.OBJECT as input, "' . $this->get_input_data_sheet()->get_meta_object()->get_alias_with_namespace() . '" given instead!');
		}
		
		$result_data_sheet = $this->exface()->data()->create_data_sheet($this->exface()->model()->get_object('exface.Core.ATTRIBUTE'));
		$skipped_columns = 0;
		foreach ($this->get_input_data_sheet()->get_rows() as $input_row){
			$target_obj = $this->exface()->model()->get_object($input_row[$this->get_input_data_sheet()->get_uid_column()->get_name()]);
			foreach ($this->get_app()->get_attribute_properties_from_table($target_obj, $input_row['DATA_ADDRESS']) as $row){
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