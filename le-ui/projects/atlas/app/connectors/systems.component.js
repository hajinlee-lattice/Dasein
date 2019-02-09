import React, { Component, react2angular } from "common/react-vendor";
import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";

import SystemComponent from './system.component';


import ConnectorService from './connectors.service';

import './systems.component.scss';
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
                let connectors = response.data.CRM;
                connectors = connectors.concat(response.data.MAP);
                this.setState({ loading: false, connectors: connectors });
                // console.log("BACK HERE ", response);
            })
        );
    }
    getColumns(rowNum, numColumns) {
        let ui = [];
        let columns = this.state.connectors.slice(rowNum * numColumns, numColumns);
        columns.forEach(element => {
            console.log('ELEMENT ===>',element);
            if(element){
                let columnsUi = (
                    <div class='le-flex-column'>
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
            return (<h4>No systems created</h4>);
        } else {
            let rows = this.state.connectors.length / 3;
            if (this.state.connectors.length % 3 > 0) {
                rows = rows + 1;
            }
            console.log('ROWS', rows);
            let ui = [];
            for (let i = 0; i < rows; i++) {
                let rowUI = (
                    <div class='le-flex-row'>
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
                <div class='some-page-wrapper'>
                    {this.getSystemsRows()}
                </div>
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