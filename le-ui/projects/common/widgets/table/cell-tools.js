import React, { Component } from "../../react-vendor";
import PropTypes from "prop-types";
import "./table.scss";

export default class CellTools extends Component {
  constructor(props) {
    super(props);
  }

  render() {
    return <li className="le-cell-tools">{this.props.children}</li>;
  }
}

