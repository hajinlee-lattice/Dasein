import React, { Component } from "common/react-vendor";
import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
import ReactRouter from '../react/router';

import GridLayout from 'common/widgets/container/grid-layout.component';

import SystemComponent from './system.component';

import SystemsService from './systems.service';
import ConnectorService from './connectors.service';
import LeVPanel from 'common/widgets/container/le-v-panel';
import './systems-list.component.scss';
import { MEDIUM_GAP } from "../../../common/widgets/container/grid-layout.component";

export default class SystemsListComponent extends Component {
    constructor(props) {
        super(props);
        this.state = { connectors: [], loading: true, generatedAuthCode: false };
        this.getConnectors = this.getConnectors.bind(this);
        let FeatureFlagService = ReactRouter.getRouter().ngservices.FeatureFlagService;
        this.alfaFeature = FeatureFlagService.FlagIsEnabled(FeatureFlagService.Flags().ALPHA_FEATURE);
        console.log('FF ', this.alfaFeature);
    }

    getConnectors(response) {
        let connectors = [];
        let CRMs = response.data.CRM || [];
        let MAPs = response.data.MAP || [];
        let FILE_SYSTEM = response.data.FILE_SYSTEM || [];
        if (this.alfaFeature) {
            connectors = FILE_SYSTEM.concat(CRMs, MAPs);
        } else {
            connectors = CRMs.concat(MAPs);
        }
        return connectors;
    }
    componentDidMount() {
        this.setState({ loading: true });
        httpService.get(
            "/pls/lookup-id-mapping",
            new Observer(response => {
                let connectors = this.getConnectors(response);
                SystemsService.cleanupLookupId(connectors);
                this.setState({ loading: false, connectors: connectors, count: connectors.length });
            })
        );
    }

    getSystems() {
        let ret = [];
        if (this.state.connectors && this.state.connectors.length > 0) {
            this.state.connectors.forEach(system => {
                // console.log(system.externalSystemName);
                ret.push((
                    <SystemComponent system={system}
                        config={{
                            img: ConnectorService.getImgByConnector(system.externalSystemName)
                        }}
                    />));
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
            <LeVPanel hstretch={'true'} className="systems-main">
                {this.getCount()}
                <GridLayout gap={MEDIUM_GAP} classNames="systems-list extends">{this.getSystems()}</GridLayout>
            </LeVPanel>
        );
    }
}