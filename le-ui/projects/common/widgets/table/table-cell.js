import React, { Component } from "../../react-vendor";
import PropTypes from "prop-types";
import "./table.scss";

export default class LeGridCell extends Component {
  constructor(props) {
    super(props);
    // console.log('CELL ', props);
  }

  getTools() {
    if (this.props.config && this.props.config.tools) {
      return (
        <li className="le-cell-tools">
          {this.props.config.tools(this.props.rowData)}
        </li>
      );
    } else {
      return null;
    }
  }
  getContent() {
    return (
      <li className="le-table-cell-content">
        <span>{this.props.value}</span>
        {this.getIcon(this.props.rowData)}
      </li>
    );
  }

  getIcon() {
    if(this.props.config && this.props.config.icon){
      return this.props.config.icon(this.props.rowData);
    }
  }

  render() {
    // console.log(this.props.colName);
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
        {this.getContent()}
        {this.getTools()}
      </ul>
    );
  }
}

LeGridCell.propTypes = {
  rowData: PropTypes.object,
  config: PropTypes.object,
  row: PropTypes.number,
  colName: PropTypes.string,
  col: PropTypes.number,
  value: PropTypes.string,
  rowState: PropTypes.string,
  overRow: PropTypes.bool
};
