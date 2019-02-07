import React, { Component, react2angular } from "common/react-vendor";

import LeTile from 'common/widgets/container/tile/le-tile';
import LeTileHeader from 'common/widgets/container/tile/le-tile-header';
import LeTileBody from 'common/widgets/container/tile/le-tile-body';
import LeMenu from 'common/widgets/menu/le-menu';
import './systems.component.scss';

export default class SystemComponent extends Component {
    constructor(props) {
        super(props);
        console.log('System', props);
        
    }

    render() {
        return (
            <LeTile classNames="profile-tile">
                <LeTileHeader>
                    <img src={this.props.img} className="systemImage"/>
                    <span className="le-tile-title">{this.props.system.orgName}</span>
                    
                    <LeMenu classNames="personalMenu" image="fa fa-ellipsis-v" name="main">

                    </LeMenu>
                </LeTileHeader>
                <LeTileBody>
                    <p>The description here</p>
                    <span>The body</span>
                </LeTileBody>
            </LeTile>
        );
    }
}
