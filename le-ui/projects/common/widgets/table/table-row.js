import React, { Component } from "../../react-vendor";
import "./table.scss";
import PropTypes from "prop-types";
import LeTableCell from "./table-cell";

export default class LeTableRow extends Component {
  constructor(props) {
    super(props);
  }
  getCells() {
    if (this.props.columnsMapping) {
      let cellsUI = Object.keys(this.props.columnsMapping).map((key, index) => {
        let column = this.props.columnsMapping[key];
        return (
          <LeTableCell
            key={index}
            jsonConfig={this.props.jsonConfig}
            columnsMapping={this.props.columnsMapping}
            colSpan={column.colSpan}
            rowIndex={this.props.rowIndex}
            colIndex={index}
            colName={column.name}
            rowData={this.props.rowData}
          />
        );
      });
      return cellsUI;
    } else {
      return null;
    }
  }

  render() {
    let rowClass = `le-table-row row-${this.props.rowIndex} ${
      this.props.rowClasses ? this.props.rowClasses : ""
    }`;
    let externalFormatting = "";
    if (this.props.formatter) {
      externalFormatting = this.props.formatter(this.props.rowData);
    }
    let format = `${rowClass} ${externalFormatting ? externalFormatting : ""}`;
    if (this.props.jsonConfig) {
      return <div className={format}>{this.getCells()}</div>;
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

      // console.log('FORM ', format);
      return <div className={format}>{childrenWithProps}</div>;
    }
  }
}

LeTableRow.PropTypes = {
  jsonConfig: PropTypes.bool,
  columnsMapping: PropTypes.object,
  rowIndex: PropTypes.number,
  rowData: PropTypes.object
};
