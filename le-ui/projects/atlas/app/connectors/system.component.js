import React, { Component, react2angular } from "common/react-vendor";

import LeTile from 'common/widgets/container/tile/le-tile';
import LeTileHeader from 'common/widgets/container/tile/le-tile-header';
import LeTileBody from 'common/widgets/container/tile/le-tile-body';
import LeTileFooter from 'common/widgets/container/tile/le-tile-footer';
import LeMenu from 'common/widgets/menu/le-menu';
import LeButton from "common/widgets/buttons/le-button";

import './systems.component.scss';

export default class SystemComponent extends Component {
    constructor(props) {
        super(props);
        console.log('System', props);
        this.editMappingClickHandler = this.editMappingClickHandler.bind(this);

    }
    editMappingClickHandler(){

    }
    getSystemStatus() {
        switch (this.props.system.isRegistered) {
            case true:
                return 'Registered';
            case false:
                return 'Unmapped';
        }
    }
    render() {
        return (
            <LeTile classNames="profile-tile">
                <LeTileHeader>
                    <img src={this.props.img} className="systemImage" />
                    <span className="le-tile-title">{this.props.system.externalSystemName}</span>

                    <LeMenu classNames="personalMenu" image="fa fa-ellipsis-v" name="main">
                    </LeMenu>
                </LeTileHeader>
                <LeTileBody classNames={'system-body'}>
                    <div className="some-table">
                        <div className="row">
                            <div className="column">
                                System Org Name
                            </div>
                            <div className="column color-blue">
                                {this.props.system.orgName}
                            </div>
                        </div>
                        <div className="row">
                            <div className="column">
                                Last Updated
                            </div>
                            <div className="column color-blue">
                                {this.props.system.updated}
                            </div>
                        </div>
                        <div className="row">
                            <div className="column">
                                Status
                            </div>
                            <div className="column">
                                {this.getSystemStatus()}
                            </div>
                        </div>
                    </div>
                </LeTileBody>
                <LeTileFooter classNames={'system-footer'}>
                    <div className="row">
                        <div className="column">
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
        );
    }
}
