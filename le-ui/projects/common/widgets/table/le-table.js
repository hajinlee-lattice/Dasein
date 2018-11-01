import React, { Component } from "../../react-vendor";
import LeGridList from "./table";
import LeGridCell from "./table-cell";
import CellContent from "./cell-content";
import LeTableHeader from "./table-header";
import LeTableBody from "./table-body";
import Aux from "../hoc/_Aux";

export default class LeTable extends Component {
  constructor(props) {
    super(props);
    this.columnsMapping = {};
    this.props.config.columns.forEach((column, index) => {
      this.columnsMapping[column.name] = column;
      column.colIndex = index;
    });
  }

  getHeader() {
    let header = this.props.config.columns.map((column, index) => {
      return (
        <LeGridCell colName={column.name} colSpan={column.colSpan}>
          {this.getHeaderTitle(column)}
        </LeGridCell>
      );
    });
    return header;
  }

  getHeaderTitle(column) {
    if (column.displayName) {
      return (
        <CellContent>
          <span>{column.displayName}</span>
        </CellContent>
      );
    } else {
      return null;
    }
  }

//   getRows() {
//     if (this.props.data && this.props.data.length > 0) {
//       let rowsUI = this.props.data.map((row, index) => {
//         return (
//           <LeGridRow key={index} index={index} rowData={row}>
//             {this.getCells(row)}
//           </LeGridRow>
//         );
//       });
//       return rowsUI;
//     } else {
//       return <div />;
//     }
//   }

//   getCells(rowData) {
//     let cellsUI = Object.keys(this.columnsMapping).map((key, index) => {
//       let column = this.columnsMapping[key];
//       return (
//         <LeGridCell
//           colName={column.name}
//           colSpan={column.colSpan}
//           row={index}
//           col={column.colIndex}
//           apply={this.props.applyChanges}
//         >
//           {this.getCellContent(rowData[column.name])}
//           {this.getCellTools(column.name)}
//           {this.getCellEditor(column.name)}
//         </LeGridCell>
//       );
//     });
//     return cellsUI;
//   }

//   getCellContent(displayName) {
//     if (displayName && !this.props.editing) {
//       return (
//         <CellContent>
//           <span title={displayName}>{displayName}</span>
//         </CellContent>
//       );
//     } else {
//       return <div />;
//     }
//   }
//   getCellTools(colName) {
//     if (this.columnsMapping[colName].tools && !this.props.editing) {
//       return <CellTools>{this.columnsMapping[colName].tools()}</CellTools>;
//     } else {
//       return <div />;
//     }
//     //   let column = this.columnsMapping[colName];
//     //   lat tools = column.tools;
//   }

//   getCellEditor(colName) {
//     if (this.columnsMapping[colName].editor && this.props.editing) {
//       <EditContainer>{this.columnsMapping[colName].editor()}</EditContainer>;
//     } else {
//       return <div />;
//     }
//   }
 
  render() {
    return (
      <Aux>
        <LeGridList
          name={this.props.config.name}
          showLoading={this.props.showLoading}
          showEmpty={this.props.showEmpty}
          emptymsg={`${
            this.props.config.emptymsg
              ? this.props.config.emptymsg
              : "There is no data"
          }`}
        >
          <LeTableHeader>{this.getHeader()}</LeTableHeader>

          <LeTableBody jsonConfig={true} columnsMapping={this.columnsMapping} data={this.props.data}/>
        </LeGridList>
      </Aux>
    );
  }
}
