import React, { Component } from "common/react-vendor";
import propTypes from "prop-types";
import "./table.scss";

import LeTableRow from "./table-row";
import Aux from "widgets/hoc/_Aux";

import {createColumnData} from './controlls/sort';
export default class LeTableHeader extends Component {
    constructor(props) {
        super(props);
        // this.data = {};
        this.data = createColumnData(this.props.headerMapping, 'displayName');
        // console.log('THE DATA ', this.data);
        // Object.keys(this.props.headerMapping).forEach(key => {
        //     // console.log(key.split('.'));
        //     // let val = getColumnData(this.props.headerMapping, key.split('.'));
        //     // // console.log('==> ', key, val);
        //     this.data[key] = this.props.headerMapping[key].displayName
        //         ? this.props.headerMapping[key].displayName
        //         : "";
        // });
    }

    getHeader() {
        return (
            <LeTableRow
                columnsMapping={this.props.headerMapping}
                rowIndex={0}
                rowData={this.data}
                rowClasses={"le-table-header"}
            >
                {this.props.children}
            </LeTableRow>
        );
    }

    render() {
        return <Aux>{this.getHeader()}</Aux>;
    }
}
LeTableHeader.propTypes = {
    headerMapping: propTypes.object.isRequired
};
