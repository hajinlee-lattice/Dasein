import React, { Component } from "../../react-vendor";
import PropTypes from "prop-types";
import "./table.scss";

export default class LeGridCell extends Component {
  constructor(props) {
    super(props);
  }

  render() {
    let span = `le-table-cell le-table-col-span-${this.props.colSpan} cell-${
      this.props.row
    }-${this.props.col} ${this.props.colName}`;
    let externalFormatting = ''
    if(this.props.config && this.props.config.formatter){
      externalFormatting = this.props.config.formatter(this.props.rowData);
    } 
    let format = `${span} ${externalFormatting}`; 
    return (
      <ul className={format}>
        {this.props.children}
      </ul>
    );
  }
}

LeGridCell.propTypes = {
  
};
