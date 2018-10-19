import React, { Component } from "../../react-vendor";
import PropTypes from "prop-types";
import "./table.scss";
import LeGridCell from "./table-cell";
import { ENABLED, DISABLED } from "./table-utils";
export default class LeGridRow extends Component {
  constructor(props) {
    super(props);
    this.updateStateRow(props.rowState);
    this.setState({ over: false });
    this.mouseEnterHandler = this.mouseEnterHandler.bind(this);
    this.mouseLeaveHandler = this.mouseLeaveHandler.bind(this);
  }

  updateStateRow(newState) {
    if (newState === DISABLED) {
      this.setState({ rowState: DISABLED });
    } else {
      this.setState({ rowState: ENABLED });
      // this.rowClass = "";
    }
  }

  mouseEnterHandler() {
    this.setState({ over: true });
  }

  mouseLeaveHandler() {
    this.setState({ over: false });
  }


  getCells() {
    let columns = this.props.config;
    let cellsUI = columns.map((column, index) => {
      return (
        <LeGridCell
          rowData={this.props.rowData}
          row={this.props.index}
          col={index}
          config={column.cell}
          value={this.props.rowData[column.name]}
          rowState={this.props.rowState}
          overRow={this.state ? this.state.over === true : false}
          colName={column.name}
          colSpan={column.numSpan}
        />
      );
    });
    return cellsUI;
  }

  render() {
    let rowClass = `le-table-row row-${this.props.index} ${this.rowClasses ? this.rowClasses : ''}`;
    let externalFormatting = ''
    if(this.props.formatter){
      externalFormatting = this.props.formatter(this.props.rowData);
    } 
    let format = `${rowClass} ${externalFormatting ? externalFormatting: ''}`;
    // console.log('FORM ', format);
    return (
      <div
        className={format}
        onMouseEnter={this.mouseEnterHandler}
        onMouseLeave={this.mouseLeaveHandler}
      >
        {this.getCells()}
      </div>
    );
  }
}
LeGridRow.propTypes = {
  config: PropTypes.object,
  formatter: PropTypes.func.isRequired,
  index: PropTypes.number,
  rowData: PropTypes.object,
  rowClasses: PropTypes.string
};
