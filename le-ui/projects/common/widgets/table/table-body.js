import React, { Component } from "common/react-vendor";
import propTypes from "prop-types";
import "./table.scss";
import LeTableRow from "./table-row";

export default class LeTableBody extends Component {
    constructor(props) {
        super(props);
    }

    getRows() {
        if (this.props.data && this.props.data.length > 0) {
            let rowsUI = this.props.data.map((row, index) => {
                return (
                    <LeTableRow
                        key={index}
                        columnsMapping={this.props.columnsMapping}
                        rowIndex={index}
                        rowData={row}
                    >
                        {this.props.children}
                    </LeTableRow>
                );
            });
            return rowsUI;
        } else {
            return null;
        }
    }

    render() {
        return (
            <div className="le-table-row le-table-body">{this.getRows()}</div>
        );
    }
}

LeTableBody.propTypes = {
    columnsMapping: propTypes.object.isRequired,
    data: propTypes.array
};
