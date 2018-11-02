import React, { Component } from "../../react-vendor";
import PropTypes from "prop-types";

import LeTableCell from "./table-cell";
import CellContent from "./cell-content";
import LeTableHeader from "./table-header";
import LeTableBody from "./table-body";
import "./table.scss";

export default class LeTable extends Component {
  constructor(props) {
    super(props);
    this.columnsMapping = {};
    this.props.config.columns.forEach((column, index) => {
      this.columnsMapping[column.name] = column;
      column.colIndex = index;
    });
  }

  getLoading() {
    if (this.props.showLoading) {
      return (
        <div className="le-table-row-no-select le-table-col-span-12 le-table-cell le-table-cell-centered">
          <i class="fa fa-spinner fa-spin fa-2x fa-fw" />
        </div>
      );
    } else {
      return null;
    }
  }
  getEmptyMsg() {
    if (this.props.showEmpty) {
      return (
        <div className="le-table-row-no-select le-table-col-span-12 le-table-cell le-table-cell-centered">
          <p>{this.props.emptymsg}</p>
        </div>
      );
    } else {
      return null;
    }
  }
  getHeader() {
    let header = this.props.config.columns.map((column, index) => {
      return (
        <LeTableCell colName={column.name} colSpan={column.colSpan}>
          {this.getHeaderTitle(column)}
        </LeTableCell>
      );
    });
    return header;
  }

  getHeaderTitle(column) {
    if (column.displayName) {
      return (
        <CellContent>
          <span>{column.displayName}</span>
        </CellContent>
      );
    } else {
      return null;
    }
  }
  render() {
    return (
      <div className={`le-table ${this.props.name}`}>
        <LeTableHeader>{this.getHeader()}</LeTableHeader>
        <LeTableBody
          jsonConfig={true}
          columnsMapping={this.columnsMapping}
          data={this.props.data}
        />
        {this.getEmptyMsg()}
        {this.getLoading()}
      </div>
    );
  }
}

LeTable.propTypes = {
  name: PropTypes.string,
  showEmpty: PropTypes.bool,
  showLoading: PropTypes.bool
};
