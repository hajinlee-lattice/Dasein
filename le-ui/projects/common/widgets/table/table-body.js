import React, { Component } from "../../react-vendor";
import PropTypes from "prop-types";
import "./table.scss";
import LeGridRow from "./table-row";

export default class LeTableBody extends Component {
  constructor(props) {
    super(props);
  }

  getRows() {
    if (this.props.data && this.props.data.length > 0) {
      let rowsUI = this.props.data.map((row, index) => {
        return (
          <LeGridRow
            key={index}
            index={index}
            rowData={row}
            jsonConfig={this.props.jsonConfig}
            columnsMapping={this.props.columnsMapping}
          >
            {this.props.children}
          </LeGridRow>
        );
      });
      return rowsUI;
    } else {
      return null;
    }
  }

  render() {
    if (this.props.jsonConfig) {
      return <div class="le-table-row le-table-body">{this.getRows()}</div>;
    } else {
      const { children } = this.props;
      const newProps = {};
      Object.keys(this.props).forEach(prop => {
        if (prop != "children") {
          newProps[prop] = this.props[prop];
        }
      });
      var childrenWithProps = React.Children.map(children, child => {
        if (child != null) {
          return React.cloneElement(child, newProps);
        }
      });
      return <div class="le-table-row le-table-body">{childrenWithProps}</div>;
    }
  }
}
