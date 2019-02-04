import React, { Component, PropTypes } from "common/react-vendor";
import LeLink from "common/widgets/link/le-link";
import Aux from "common/widgets/hoc/_Aux";

export const CREATE_TEMPLATE = 'create-template';
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
    if (!this.props.rowData.Exist) {
      return (
        <LeLink
          config={{
            label: "Create Template",
            classes: "borders-over le-blu-link",
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
    } else {
      return null;
    }
  }

  getEditButton() {
    if (this.props.rowData.Exist) {
      return (
        <LeLink
          config={{
            label: "Edit Template",
            classes: "borders-over le-blu-link edit-button",
            name: ""
          }}
          callback={() => {
            this.props.callback({
                action: EDIT_TEMPLATE, 
                type: this.props.rowData.Object, 
                data: this.props.rowData
            });
          }}
        />
      );
    } else {
      return null;
    }
  }

  getImportButton() {
    if (this.props.rowData.Exist) {
      return (
        <LeLink
          config={{
            label: "Import Data",
            classes: "borders-over le-blu-link import-button",
            name: ""
          }}
          callback={() => {
            this.props.callback({
                action: IMPORT_DATA, 
                type:  this.props.rowData.Object, 
                data: this.props.rowData
            });
          }}
        />
      );
    } else {
      return null;
    }
  }

  render() {
    return (
      <Aux>
        {this.getCreateButton()}
        {this.getEditButton()}
        {this.getImportButton()}
      </Aux>
    );
  }
}
