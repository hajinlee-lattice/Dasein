import React, { Component } from '../../../common/react-vendor';
import { Toolbar } from '../../../common/widgets/toolbar/le-toolbar';
import Aux from '../../../common/widgets/hoc/_Aux';
import '../../../common/widgets/layout/layout.scss';
import {LeToolBar} from '../../../common/widgets/buttons/le-button';

export default class DropdownPage extends Component {
    constructor(props) {
        super(props);
        console.log('Component initialized');
    }

    render() {
        const config = {
            lable: "Test",
            classNames: ['button', 'orange-button']
        }
        return (
            <Aux>
                <LeToolBar>
                    <div className="left">
                        <LeButton config={config}></LeButton>
                        <LeButton config={config}></LeButton>
                        <LeButton config={config}></LeButton>
                        <LeButton config={config}></LeButton>
                        <LeButton config={config}></LeButton>
                        <LeButton config={config}></LeButton>
                    </div>
                    <div className="right">
                        <LeButton config={config}></LeButton>
                        <LeButton config={config}></LeButton>
                        <LeButton config={config}></LeButton>
                        <LeButton config={config}></LeButton>
                        <LeButton config={config}></LeButton>
                    </div>
                </LeToolBar>
            </Aux>
        );
    }
}