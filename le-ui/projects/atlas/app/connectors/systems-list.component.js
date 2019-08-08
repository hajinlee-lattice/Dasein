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
        this.alphaFeature = FeatureFlagService.FlagIsEnabled(FeatureFlagService.Flags().ALPHA_FEATURE);
        this.linkedInEnabled = FeatureFlagService.FlagIsEnabled(FeatureFlagService.Flags().ENABLE_LINKEDIN_INTEGRATION);
        this.facebookEnabled = FeatureFlagService.FlagIsEnabled(FeatureFlagService.Flags().ENABLE_FACEBOOK_INTEGRATION);
        console.log('FF ', this.alphaFeature);
        console.log('FF ', this.linkedInEnabled);
        console.log('FF ', this.facebookEnabled);
    }

    getConnectors(response) {
        let connectors = [];
        let CRMs = response.data.CRM || [];
        let MAPs = response.data.MAP || [];
        let ADSs = response.data.ADS || [];
        let FILE_SYSTEM = response.data.FILE_SYSTEM || [];

        connectors = this.alphaFeature ? connectors.concat(FILE_SYSTEM) : connectors;
        connectors = connectors.concat(CRMs, MAPs);
        connectors = this.linkedInEnabled || this.facebookEnabled ? connectors.concat(ADSs) : connectors;
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