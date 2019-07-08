import React, { Component, PropTypes } from "common/react-vendor";
import LeLink from "common/widgets/link/le-link";
import LeButton from "common/widgets/buttons/le-button";
import LeMenu from "common/widgets/menu/le-menu";
import LeMenuItem from "common/widgets/menu/le-menu-item";

import Aux from "common/widgets/hoc/_Aux";

export const CREATE_TEMPLATE = 'create-template';
export const VIEW_TEMPLATE = 'view-template';
export const EDIT_TEMPLATE = 'edit-template';
export const IMPORT_DATA = 'import-data';

export const response = {
    action: '',
    type: ''
}

export default class TemplatesRowControlles extends Component {
  constructor(props) {
    super(props);
  }

  getCreateButton() {
    return (
      <LeButton
        config={{
          label: "Create Template",
          classNames: "button white-button",
          name: ""
        }}
        callback={() => {
          this.props.callback({
              action: CREATE_TEMPLATE, 
              type:  this.props.rowData.Object, 
              data: this.props.rowData
          });
        }}
      />
    );
  }

  getViewButton() {
    return (
      <LeMenuItem
        name="edit"
        label="View Template"
        image="fa fa-list-alt"
        callback={name => {
          this.props.callback({
              action: VIEW_TEMPLATE, 
              type: this.props.rowData.Object, 
              data: this.props.rowData
          });
        }}
      />
    );  
  }

  getEditButton() {
    return (
      <LeMenuItem
        name="edit"
        label="Edit Template"
        image="fa fa-pencil-square-o"
        callback={name => {
          this.props.callback({
              action: EDIT_TEMPLATE, 
              type: this.props.rowData.Object, 
              data: this.props.rowData
          });
        }}
      />
    );  
  }

  getImportButton() {
    return (
      <LeMenuItem
        name="import"
        label="Do a One-off Import"
        image="fa fa-upload"
        callback={name => {
          this.props.callback({
              action: IMPORT_DATA, 
              type:  this.props.rowData.Object, 
              data: this.props.rowData
          });
        }}
      />
    );
  }

  render() {
    if (!this.props.rowData.Exist) {
      return (
        <Aux>
          {this.getCreateButton()}
        </Aux>
      );
    } else {
      return (
        <LeMenu classNames="test-menu" image="fa fa-ellipsis-v" name="main">
          {this.getViewButton()}
          {this.getEditButton()}
          {this.getImportButton()}
        </LeMenu>
      )
    }

  }
}
