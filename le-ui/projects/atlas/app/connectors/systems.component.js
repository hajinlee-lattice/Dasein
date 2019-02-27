import React, { Component, react2angular } from "common/react-vendor";
import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";

import SystemComponent from './system.component';

import SystemsService from './systems.service';
import ConnectorService from './connectors.service';

import './systems.component.scss';
import '../../../common/widgets/layout/le-layouts.scss';
export default class SystemsComponent extends Component {
    constructor(props) {
        super(props);
        console.log('Systems ', props.systems);
        this.state = { connectors: [] };
    }
    componentDidMount() {
        httpService.get(
            "/pls/lookup-id-mapping",
            new Observer(response => {
                let CRMs = response.data.CRM || [];
                let MAPs = response.data.MAP || [];
                let connectors = CRMs.concat(MAPs);
                SystemsService.cleanupLookupId(connectors);
                this.setState({ loading: false, connectors: connectors });
                // console.log("BACK HERE ", response);
            })
        );
    }
    getColumns(rowNum, numColumns) {
        let ui = [];
        let columns = [];
        if((rowNum * numColumns + numColumns) >= this.state.connectors.length){
            columns = this.state.connectors.slice(rowNum * numColumns);
        }else{
            columns = this.state.connectors.slice(rowNum * numColumns, numColumns);
        }
        columns.forEach(element => {
            console.log('ELEMENT ===>',element);
            if(element){
                let columnsUi = (
                    <div class='le-layout-flex-col'>
                        <SystemComponent system={element} img={ConnectorService.getImgByConnector(element.externalSystemName)}/>
                    </div>
                );
                ui.push(columnsUi);
            }
        });
        return ui;

    }
    getSystemsRows() {
        if (this.state.connectors.length == 0) {
            return (<h5>No systems created</h5>);
        } else {
            let rows = this.state.connectors.length / 3;
            if (this.state.connectors.length % 3 > 0) {
                rows = rows + 1;
            }
            console.log('ROWS', rows);
            let ui = [];
            for (let i = 0; i < rows; i++) {
                let rowUI = (
                    <div class='le-layout-flex-grid'>
                        {this.getColumns(i, 3)}
                    </div>
                );
                ui.push(rowUI);
            }
            return ui;
        }
    }

    render() {
        return (
            <div className="systems-main">
                {/* <div class='some-page-wrapper'> */}
                    {this.getSystemsRows()}
                {/* </div> */}
            </div>
        );
    }
}

angular
    .module("le.systems.list", [])

    .component(
        "systemsComponent",
        react2angular(SystemsComponent, [], ['$state'])
    );