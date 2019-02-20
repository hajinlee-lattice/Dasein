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

import './systems.component.scss';

export default class SystemComponent extends Component {
    constructor(props) {
        super(props);
        console.log('System', props.system);
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
                return 'Registered';
            case false:
                return 'Unmapped';
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
        }else{
            return '-';
        }
    }


    render() {
        // console.log('Render', this.state.openModal);
        return (
            <Aux>
                <LeModal opened={this.state.openModal} callback={this.modalCallback} title="Org ID to Account ID Mapping" template={this.getEditTemplate} />
                <LeTile classNames="system-tile">
                    <LeTileHeader classNames="system-title">
                        <div className="system-image-container">
                            <img src={this.props.img} className="systemImage" />
                        </div>
                        <div className="system-title-container">
                            <span className="le-tile-title">{this.state.system.externalSystemName}</span>
                        </div>
                    </LeTileHeader>
                    <LeTileBody classNames={'system-body'}>
                        <div className="le-layout-flex-grid">
                            <div className="le-layout-flex-col lable">
                                System Org Name
                                </div>
                            <div className="le-layout-flex-col color-blue content">
                                {this.state.system.orgName}
                            </div>
                        </div>
                        <div className="le-layout-flex-grid">
                            <div className="le-layout-flex-col lable">
                                System Org Id
                            </div>
                            <div className="le-layout-flex-col color-blue content">
                                {this.state.system.orgId}
                            </div>
                        </div>
                        <div className="le-layout-flex-grid">
                            <div className="le-layout-flex-col lable">
                                Account Id
                            </div>
                            <div className="le-layout-flex-col color-blue content" title={this.state.system.accountId}>
                                {this.state.system.accountId}
                            </div>
                        </div>
                        <div className="le-layout-flex-grid">
                            <div className="le-layout-flex-col lable">
                                Last Updated
                            </div>
                            <div className="le-layout-flex-col color-blue content">
                                {this.getValueDateFormatted(this.state.system.updated)}
                            </div>
                        </div>
                        <div className="le-layout-flex-grid">
                            <div className="le-layout-flex-col lable">
                                Status
                            </div>
                            <div className="le-layout-flex-col content">
                                <span className={this.getSystemStatusClass()}>{this.getSystemStatus()}</span>
                            </div>
                        </div>
                    </LeTileBody>
                    <LeTileFooter classNames={'system-footer right-controlls'}>

                        <LeButton
                            name={`${"edit-mappings-"}${this.state.system.orgName}`}
                            disabled={this.state.saving || !this.state.system.isRegistered}
                            config={{
                                label: "Edit Mappings",
                                classNames: "blue-button"
                            }}
                            callback={this.editMappingClickHandler}
                        />
                    </LeTileFooter>
                </LeTile>
            </Aux>
        );
    }
}
