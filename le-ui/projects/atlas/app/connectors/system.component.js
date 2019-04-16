import React, { Component, react2angular } from "common/react-vendor";

import Aux from 'common/widgets/hoc/_Aux';
import LeTile from 'common/widgets/container/tile/le-tile';
import LeTileHeader from 'common/widgets/container/tile/le-tile-header';
import LeTileBody from 'common/widgets/container/tile/le-tile-body';
import LeTileFooter from 'common/widgets/container/tile/le-tile-footer';
import LeButton from "common/widgets/buttons/le-button";
import LeModal from "common/widgets/modal/le-modal";
import SystemMappingComponent from './system-mapping.component';
import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
import ConnectorService, { MARKETO, SALESFORCE, ELOQUA } from './connectors.service';

import './systems.component.scss';
import LeHPanel from "common/widgets/container/le-h-panel";
import GridLayout from 'common/widgets/container/grid-layout.component';
import { RIGHT, CENTER } from "common/widgets/container/le-alignments";

export default class SystemComponent extends Component {
    constructor(props) {
        super(props);
        this.state = { system: props.system, openModal: false, saving: false };
        this.editMappingClickHandler = this.editMappingClickHandler.bind(this);
        this.modalCallback = this.modalCallback.bind(this);
        this.getEditTemplate = this.getEditTemplate.bind(this);
        this.editMapping = Object.assign({}, props.system);
        this.mappingClosed = this.mappingClosed.bind(this);

    }
    editMappingClickHandler() {
        console.log('Clicked');
        this.setState({ openModal: true });
    }

    mappingClosed(system) {
        if (this.state.saving) {
            let observer = new Observer(
                response => {
                    // httpService.printObservables();
                    console.log('HEY ', response);
                    if (response.data) {
                        let tmp = response.data;
                        this.setState({ saving: false, system: tmp });
                        httpService.unsubscribeObservable(observer);
                    }
                }
            );

            httpService.put((('/pls/lookup-id-mapping/config/' + this.state.system.configId)), system, observer);
        }
    }

    modalCallback(action) {

        // console.log('CHANGED? =====>',this.editMapping);
        switch (action) {
            case 'close':
                this.setState({ openModal: false });
                break;
            case 'ok':
                this.setState({ openModal: false, saving: true });
                break;
        }
    }


    getSystemStatus() {
        switch (this.state.system.isRegistered) {
            case true:
                return 'Connected';
            case false:
                return 'Disconnected';
        }
    }

    getSystemStatusClass() {
        let color = 'color-';
        switch (this.state.system.isRegistered) {
            case true:
                color = `${color}${'green'}`;
                break;
            default:
                color = `${color}${'red'}`;
                break;
        }
        return color;

    }

    getEditTemplate() {
        console.log('TEMPLATE ', this.editMapping);
        this.editMapping = Object.assign({}, this.state.system);
        return (
            <SystemMappingComponent system={this.editMapping} closed={this.mappingClosed} />
        );
    }

    getValueDateFormatted(longValue) {
        if (longValue && longValue != null) {
            var options = {
                year: "numeric",
                month: "2-digit",
                day: "2-digit"
            };
            var formatted = new Date(longValue);

            var ret = "err";
            try {
                ret = formatted.toLocaleDateString(
                    "en-US",
                    options
                );
            } catch (e) {
                console.log(e);
            }

            return ret;
        } else {
            return '-';
        }
    }

    getAccountIdRow() {
        if (this.props.system.externalSystemType != "MAP") {

            return (
                <Aux>
                    <span className="s-label">Account Id:</span>
                    <span className="s-text" title={this.state.system.accountId}>{this.state.system.accountId}</span>
                </Aux>
            );
        }
        return null;
    }


    render() {
        // console.log('Render', this.state.openModal);
        return (
            <Aux>
                <LeModal opened={this.state.openModal} callback={this.modalCallback} title="Org ID to Account ID Mapping" template={this.getEditTemplate} />
                <LeTile>
                    <LeTileHeader>
                        <LeHPanel valignment={CENTER}>
                            <img src={this.props.img} className="s-image" />
                            <p className="s-title">{this.state.system.externalSystemName}</p>
                        </LeHPanel>
                    </LeTileHeader>
                    <LeTileBody classNames={"s-body"}>
                        <GridLayout classNames="system-body-container">
                            <span className="s-label">System Org Name:</span>
                            <span className="s-text">{this.state.system.orgName}</span>
                            <span className="s-label">System Org Id:</span>
                            <span className="s-text">{this.state.system.orgId}</span>
                            {this.getAccountIdRow()}
                            <span className="s-label">Last Updated:</span>
                            <span className="s-text">{this.getValueDateFormatted(this.state.system.updated)}</span>
                            <span className="s-label">Status:</span>
                            <span className={`${'s-text'} ${this.getSystemStatusClass()}`}>{this.getSystemStatus()}</span>
                        </GridLayout>
                    </LeTileBody>
                    <LeTileFooter>
                        <LeHPanel hstretch={true} halignment={RIGHT} classesName="s-controls">
                            <LeButton
                                name={`${"edit-mappings-"}${this.state.system.orgName}`}
                                disabled={this.state.saving || !this.state.system.isRegistered}
                                config={{
                                    label: "Edit Mappings",
                                    classNames: "blue-button connections-edit-mappings"
                                }}
                                callback={this.editMappingClickHandler}
                            /></LeHPanel>
                    </LeTileFooter>
                </LeTile>
            </Aux>
        );
    }
}
