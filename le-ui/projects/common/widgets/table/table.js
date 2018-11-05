import React, { Component } from "../../react-vendor";
import propTypes from "prop-types";

import LeTableHeader from "./table-header";
import LeTableBody from "./table-body";
import "./table.scss";

export default class LeTable extends Component {
  constructor(props) {
    super(props);
    this.columnsMapping = {};
    this.headerMapping = {};
    this.props.config.header.forEach((header, index) => {
      this.headerMapping[header.name] = Object.assign({},header);
      this.headerMapping[header.name].colSpan = this.props.config.columns[index].colSpan;
      const newItem = Object.assign(header, this.props.config.columns[index]);
      newItem.colIndex = index;
      this.columnsMapping[header.name] = newItem;
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

  render() {
    return (
      <div className={`le-table ${this.props.name}`}>
        <LeTableHeader headerMapping={this.headerMapping} />
        <LeTableBody
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
  name: propTypes.string.isRequired,
  config: propTypes.object.isRequired,
  data: propTypes.array.isRequired,
  showEmpty: propTypes.bool,
  showLoading: propTypes.bool,
  

};
