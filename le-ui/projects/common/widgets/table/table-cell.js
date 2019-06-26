import React, { Component } from "common/react-vendor";
import propTypes from "prop-types";
import "./table.scss";
import CellContent from "./cell-content";
import {getColumnData} from './controlls/sort';

export default class LeTableCell extends Component {
    constructor(props) {
        super(props);

        this.state = { editing: false, saving: false };
        this.toogleEdit = this.toogleEdit.bind(this);
        this.cancelHandler = this.cancelHandler.bind(this);
    }
    cancelHandler() {
        this.toogleEdit();
    }

    setSavingState() {
        this.setState({ saving: true });
    }
    getSaving() {
        if (this.state.saving) {
            return (
                <div>
                    <i class="fa fa-spinner fa-spin fa-fw" />
                </div>
            );
        } else {
            return null;
        }
    }

    toogleSaving() {
        this.setState({ saving: !this.state.saving });
    }
    toogleEdit() {
        this.setState({ editing: !this.state.editing, saving: false });
    }

    getContentFormated(content) {
        if (this.props.columnsMapping[this.props.colName].contenFormatter) {
            return this.props.columnsMapping[
                this.props.colName
            ].contenFormatter(content);
        } else {
            return content;
        }
    }
    getCellContent() {
        let displayName = getColumnData(this.props.rowData, this.props.colName.split('.'))//this.props.rowData[this.props.colName];
        
        if (displayName && !this.state.editing && this.props.columnsMapping[this.props.colName].onlyTemplate != true) {
            // console.log('================');
            // // console.log(displayName);
            // console.log(this.props.rowData);
            // // console.log(this.props.colName);
            // // console.log(this.props.columnsMapping);
            // console.log('================');
            return (
                <CellContent
                    value={displayName}
                    mask={this.props.columnsMapping[this.props.colName].mask}
                />
            );
        } else {
            // console.log('================');
            // console.log(displayName);
            // console.log(this.props.rowData);
            // console.log(this.props.colName);
            // console.log('================');
            return null;
        }
    }

    getTemplate() {
        if (this.props.columnsMapping[this.props.colName].template) {
            return (
                <div className={`le-cell-template ${this.props.colName}`}>
                    {this.props.columnsMapping[this.props.colName].template(
                        this,
                        this.props.rowData
                    )}
                </div>
            );
        } else {
            return null;
        }
    }

    render() {
        let span = `le-table-cell le-table-col-span-${
            this.props.columnsMapping[this.props.colName].colSpan
        } cell-${this.props.rowIndex}-${this.props.colIndex} ${
            this.props.colName
        }`;
        let externalFormatting = "";
        if (this.props.columnsMapping && this.props.columnsMapping.formatter) {
            externalFormatting = this.props.columnsMapping.formatter(
                this.props.rowData
            );
        }
        let format = `${span} ${externalFormatting}`;
        return (
            <ul className={format} onClick={() => {this.props.onClick ? this.props.onClick(this.props.rowIndex, this.props.rowData): null}}>
                {this.getCellContent()}
                {this.getTemplate()}
                {this.getSaving()}
            </ul>
        );
    }
}

LeTableCell.propTypes = {
    columnsMapping: propTypes.object.isRequired,
    colSpan: propTypes.number,
    rowIndex: propTypes.number,
    colIndex: propTypes.number,
    colName: propTypes.string,
    rowData: propTypes.object
};
