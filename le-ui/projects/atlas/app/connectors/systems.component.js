import React, { Component, react2angular } from "common/react-vendor";
import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";

import GridLayout from 'common/widgets/container/grid-layout.component';

import SystemComponent from './system.component';

import SystemsService from './systems.service';
import ConnectorService from './connectors.service';

import './systems.component.scss';
import '../../../common/widgets/layout/le-layouts.scss';
export default class SystemsComponent extends Component {
    constructor(props) {
        super(props);
        this.state = { connectors: [], loading: true };
    }

    componentDidMount() {
        httpService.get(
            "/pls/lookup-id-mapping",
            new Observer(response => {
                let CRMs = response.data.CRM || [];
                let MAPs = response.data.MAP || [];
                let connectors = CRMs.concat(MAPs);
                SystemsService.cleanupLookupId(connectors);
                this.setState({ loading: false, connectors: connectors, count: connectors.length });
            })
        );
    }

    getSystems() {
        let ret = [];
        if (this.state.connectors && this.state.connectors.length > 0) {
            this.state.connectors.forEach(system => {
                ret.push((<SystemComponent system={system} img={ConnectorService.getImgByConnector(system.externalSystemName)} />));
            });

        } else {
            switch (this.state.loading) {
                case true:
                    ret.push(<div></div>)
                    ret.push(<div style={{ display: 'flex', justifyContent: 'center' }}><i className="fa fa-spinner fa-spin fa-2x fa-fw" /></div>);
                    break;
            }
        }

        return ret;

    }
    getCount() {
        if (!this.state.loading) {
            return (<h2 className="systems-header"><strong className="systems-count">{this.state.count}</strong><span>connections</span></h2>);
        } else {
            return null;
        }

    }
    render() {
        return (
            <div className="systems-main">
                {this.getCount()}
                <GridLayout classNames="systems-list">{this.getSystems()}</GridLayout>
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