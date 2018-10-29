import React, { Component } from "../../react-vendor";
import PropTypes from "prop-types";
import "./table.scss";

export default class CellContent extends Component {
  constructor(props) {
    super(props);
  }

  render() {
    if(this.props.editing === true){
      return null;
    }
    else {
      return <li className="le-table-cell-content">{this.props.children}</li>;
    }
  }
}
