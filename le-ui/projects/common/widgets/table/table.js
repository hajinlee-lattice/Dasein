import React, { Component } from "../../react-vendor";
import PropTypes from "prop-types";
import "./table.scss";

export default class LeGridList extends Component {
  constructor(props) {
    super(props);
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
        {this.props.children}
        {this.getEmptyMsg()}
        {this.getLoading()}
      </div>
    );
  }
}

LeGridList.propTypes = {
  name: PropTypes.string,
  showEmpty: PropTypes.bool,
  showLoading: PropTypes.bool
};
