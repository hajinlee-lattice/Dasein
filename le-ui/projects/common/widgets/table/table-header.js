import React, { Component } from "../../react-vendor";
import PropTypes from "prop-types";
import "./table.scss";

export default class LeTableHeader extends Component {
  constructor(props) {
    super(props);
  }

  render() {
    return (
      <div class="le-table-header le-table-row-no-select">
        {this.props.children}
      </div>
    );
  }
}
