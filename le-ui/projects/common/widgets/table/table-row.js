import React, { Component } from "../../react-vendor";
import "./table.scss";
import LeTableCell from "./table-cell";

export default class LeTableRow extends Component {
  constructor(props) {
    super(props);
  }
  getCells() {
    let cellsUI = Object.keys(this.props.columnsMapping).map((key, index) => {
      let column = this.props.columnsMapping[key];
      return (
        <LeTableCell
          jsonConfig={this.props.jsonConfig}
          columnsMapping={this.props.columnsMapping}
          colName={column.name}
          colSpan={column.colSpan}
          row={index}
          col={column.colIndex}
          data={this.props.rowData[column.name]}
          rowData={this.props.rowData}
          apply={this.props.applyChanges}
        />
      );
    });
    return cellsUI;
  }

  render() {
    let rowClass = `le-table-row row-${this.props.index} ${
      this.rowClasses ? this.rowClasses : ""
    }`;
    let externalFormatting = "";
    if (this.props.formatter) {
      externalFormatting = this.props.formatter(this.props.rowData);
    }
    let format = `${rowClass} ${
      externalFormatting ? externalFormatting : ""
    }`;
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
