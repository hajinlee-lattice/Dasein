import React, { Component } from "../../react-vendor";
import Aux from '../hoc/_Aux';
import "./le-layouts.scss";

class LeFlexRow extends Component {
    constructor(props) {
        super(props);
        console.log(props.columnsNumber);

    }

    getColumns() {
        return (
            <div className="le-layout-flex-col">

            </div>
        );
    }

    getRow() {
        return (
            <div className="le-layout-flex-grid">
                {this.getColumns()}
            </div>);
    }

    getRows() {
        let rowsUI = [];
        let templates = this.props.getTemplates();

        return rowsUI;
        
    }


    render() {

        return (
            <Aux>
                <div className="le-layout-flex-grid">
                    <div className="le-layout-flex-col">

                    </div>
                </div>
            </Aux>
        );
    }
}
export default LeHPanel;
