import React, { Component } from "../../react-vendor";
import PropTypes from "prop-types";
import "./table.scss";
import CellContent from "./cell-content";


export default class LeGridCell extends Component {
  constructor(props) {
    super(props);

    this.state = { editing: false, saving: false };
    this.toogleEdit = this.toogleEdit.bind(this);
    this.cancelHandler = this.cancelHandler.bind(this);
  }
  cancelHandler() {
    this.toogleEdit();
  }
  
  setSavingState(){
    this.setState({ saving: true });
  }
  getSaving() {
    if (this.state.saving) {
      return (
        <div>
          <i class="fa fa-spinner fa-spin fa-fw" />
        </div>
      );
    } else {
      return null;
    }
  }

  toogleSaving() {
    this.setState({ saving: !this.state.saving });
  }
  toogleEdit() {
    this.setState({ editing: !this.state.editing, saving: false });
  }

  getCellContent() {
    let displayName = this.props.data;
    console.log("DISPLAY NAME ", displayName);
    if (displayName && !this.state.editing) {
      return (
        <CellContent>
          <span title={displayName}>{displayName}</span>
        </CellContent>
      );
    } else {
      return null;
    }
  }
  
  getTemplate() {
    if (
      this.props.columnsMapping[this.props.colName].template
    ) {
      return (
      <div className="le-cell-template" >
        {this.props.columnsMapping[this.props.colName].template(
            this, this.props.rowData
          )}
      </div>);
    } else {
      return null;
    }
  }

  render() {
    let span = `le-table-cell le-table-col-span-${this.props.colSpan} cell-${
      this.props.row
    }-${this.props.col} ${this.props.colName}`;
    let externalFormatting = "";
    if (this.props.config && this.props.config.formatter) {
      externalFormatting = this.props.config.formatter(this.props.rowData);
    }
    let format = `${span} ${externalFormatting}`;
    if (this.props.jsonConfig) {
      return (
        <ul className={format}>
          {this.getCellContent()}
          {this.getTemplate()}
          {/* {this.getCellTools()}
          {this.getCellEditor()} */}
          {this.getSaving()}
        </ul>
      );
    } else {
      const { children } = this.props;
      const newProps = {};
      Object.keys(this.props).forEach(prop => {
        if (prop != "children") {
          newProps[prop] = this.props[prop];
        }
      });
      newProps.cancel = this.toogleEdit;
      newProps.toogleEdit = this.toogleEdit;
      newProps.editing = this.state.editing;
      newProps.saving = this.state.saving;
      // newProps.applyChanges = this.saveHandler;
      var childrenWithProps = React.Children.map(children, child => {
        if (child != null) {
          return React.cloneElement(child, newProps);
        }
      });

      return (
        <ul className={format}>
          <div>{childrenWithProps}</div>
          {this.getSaving()}
        </ul>
      );
    }
  }
}

LeGridCell.propTypes = {};
