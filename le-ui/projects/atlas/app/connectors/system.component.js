import React, { Component, react2angular } from "common/react-vendor";

import Aux from 'common/widgets/hoc/_Aux';
import LeTile from 'common/widgets/container/tile/le-tile';
import LeTileHeader from 'common/widgets/container/tile/le-tile-header';
import LeTileBody from 'common/widgets/container/tile/le-tile-body';
import LeTileFooter from 'common/widgets/container/tile/le-tile-footer';
import LeMenu from 'common/widgets/menu/le-menu';
import LeButton from "common/widgets/buttons/le-button";
import LeModal from "common/widgets/modal/le-modal";
import SystemMappingComponent from './system-mapping.component';

import './systems.component.scss';

export default class SystemComponent extends Component {
    constructor(props) {
        super(props);
        console.log('System', props);
        this.state = { openModal: false };
        this.editMappingClickHandler = this.editMappingClickHandler.bind(this);
        this.modalCallback = this.modalCallback.bind(this);
        this.getEditTemplate = this.getEditTemplate.bind(this);

    }
    editMappingClickHandler() {
        console.log('Clicked');
        this.setState({ openModal: true });
    }

    modalCallback(action) {
        switch (action) {
            case 'close':
                this.setState({ openModal: false });
                break;
            case 'ok':
                this.setState({ openModal: false });
                break;
        }
    }

    getSystemStatus() {
        switch (this.props.system.isRegistered) {
            case true:
                return 'Registered';
            case false:
                return 'Unmapped';
        }
    }
    getEditTemplate() {
        console.log('TEMPLATE');
        return (
            <SystemMappingComponent system={this.props.system} />
            // <p>T</p>
        );
    }
    render() {
        console.log('Render', this.state.openModal);
        return (
            <Aux>
                <LeModal opened={this.state.openModal} callback={this.modalCallback} title="Org ID to Account ID Mapping" template={this.getEditTemplate} />
                <LeTile classNames="system-tile">
                    <LeTileHeader classNames="system-title">
                        <div className="system-image-container">
                            <img src={this.props.img} className="systemImage" />
                        </div>
                        <div className="system-title-container">
                            <span className="le-tile-title">{this.props.system.externalSystemName}</span>
                        </div>
                    </LeTileHeader>
                    <LeTileBody classNames={'system-body'}>
                        <div className="some-table">
                            <div className="le-flex-row">
                                <div className="le-flex-column">
                                    System Org Name
                            </div>
                                <div className="le-flex-column color-blue">
                                    {this.props.system.orgName}
                                </div>
                            </div>
                            <div className="le-flex-row">
                                <div className="le-flex-column">
                                    Last Updated
                            </div>
                                <div className="le-flex-column color-blue">
                                    {this.props.system.updated}
                                </div>
                            </div>
                            <div className="le-flex-row">
                                <div className="le-flex-column">
                                    Status
                            </div>
                                <div className="le-flex-column">
                                    {this.getSystemStatus()}
                                </div>
                            </div>
                        </div>
                    </LeTileBody>
                    <LeTileFooter classNames={'system-footer'}>
                        <div className="le-flex-row">
                            <div className="le-flex-column">
                                <div className="right-controlls">
                                    <LeButton
                                        name={`${"edit-mappings-"}${this.props.system.orgName}`}
                                        config={{
                                            label: "Edit Mappings",
                                            classNames: "blue-button"
                                        }}
                                        callback={this.editMappingClickHandler}
                                    />
                                </div>
                            </div>
                        </div>
                    </LeTileFooter>
                </LeTile>
            </Aux>
        );
    }
}
