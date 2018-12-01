import React, { Component } from "common/react-vendor";
import "./table.scss";
import propTypes from "prop-types";
import LeTableCell from "./table-cell";

export default class LeTableRow extends Component {
    constructor(props) {
        super(props);
    }
    getCells() {
        if (this.props.columnsMapping) {
            let cellsUI = Object.keys(this.props.columnsMapping).map(
                (key, index) => {
                    let column = this.props.columnsMapping[key];
                    return (
                        <LeTableCell
                            key={index}
                            columnsMapping={this.props.columnsMapping}
                            colSpan={column.colSpan}
                            rowIndex={this.props.rowIndex}
                            colIndex={index}
                            colName={column.name}
                            rowData={this.props.rowData}
                        />
                    );
                }
            );
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
        let format = `${rowClass} ${
            externalFormatting ? externalFormatting : ""
        }`;
        return <div className={format}>{this.getCells()}</div>;
    }
}

LeTableRow.propTypes = {
    columnsMapping: propTypes.object.isRequired,
    rowIndex: propTypes.number,
    rowData: propTypes.object
};
