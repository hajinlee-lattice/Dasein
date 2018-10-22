import React, { Component } from "../../react-vendor";
import PropTypes from "prop-types";
import "./table.scss";

export default class CellTools extends Component {
  constructor(props) {
    super(props);
  }

  render() {
    let cellClasses = `le-cell-tools ${this.props.classes ? this.props.classes : ''}`;
    return <li className={cellClasses}>{this.props.children}</li>;
  }
}

