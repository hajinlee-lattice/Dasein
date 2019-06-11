import React, { Component } from "common/react-vendor";
import LeVPanel from 'common/widgets/container/le-v-panel.js'
import ReactMainContainer from "../../../react/react-main-container";
import ReactRouter from '../../../react/router';
import './match-priority.component.scss';
import LeHPanel from "../../../../../common/widgets/container/le-h-panel";
import LeButton from "common/widgets/buttons/le-button";

export default class MatchPriorityComponent extends Component {
    constructor(props) {
        super(props);
        console.log('MATCH');
    }

    componentDidMount() {
        console.log('MATCH MOUNTED');

    }
    componentWillUnmount() {

    }


    render() {
        return (
            <ReactMainContainer className="match-priority">
                <h2>Updating Match Priority​</h2>
                <LeVPanel hstretch={true} vstretch={true} className="wizard-container">
                    <p className="sub-header">Update Match Priority​</p>

                    <LeHPanel hstretch={"true"} >
                        <p className="left">System Priority</p>
                        <LeVPanel className="priorities">
                            The box here
                        </LeVPanel>
                        <div className="info">MATCH PRIORITY</div>
                    </LeHPanel>
                </LeVPanel>
            </ReactMainContainer>
        );
    }
}
