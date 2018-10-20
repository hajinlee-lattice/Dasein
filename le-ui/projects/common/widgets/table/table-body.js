import React, { Component } from "../../react-vendor";
import PropTypes from "prop-types";
import "./table.scss";

export default class LeTableBody extends Component {
  constructor(props) {
    super(props);
  }

  render() {
    return (
      <div class="le-table-row le-table-body">
        {this.props.children}
      </div>
    );
  }
}
