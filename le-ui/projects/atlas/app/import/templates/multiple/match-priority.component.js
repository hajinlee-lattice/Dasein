import React, { Component } from "common/react-vendor";
import LeVPanel from 'common/widgets/container/le-v-panel.js'
import ReactMainContainer from "../../../react/react-main-container";
import ReactRouter from '../../../react/router';
import './match-priority.component.scss';
import LeHPanel from "../../../../../common/widgets/container/le-h-panel";
import LeButton from "common/widgets/buttons/le-button";
import PrioritiesLitComponent from "./priorities-list.component";
import { SPACEBETWEEN } from "../../../../../common/widgets/container/le-alignments";

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
                        <PrioritiesLitComponent/>
                        <LeVPanel className="info">
                            <p>MATCH PRIORITY</p>
                            <span>If any of the systems provide multiple IDs, Lattice will use the ID with the highest priority while matching accounts and contacts</span>
                        </LeVPanel>
                    </LeHPanel>
                    <LeHPanel hstretch={true} halignment={SPACEBETWEEN} className="priorities-controlls">
                    <LeButton
                        name="cancel"
                        config={{
                            label: "Cancel",
                            classNames: "gray-button"
                        }}
                        callback={() => {
                            ReactRouter.getStateService().go('templateslist');
                            // alert('Call APIS');
                        }}
                    />
                     <LeButton
                        name="update"
                        config={{
                            label: "Update",
                            classNames: "blue-button"
                        }}
                        callback={() => {
                            ReactRouter.getStateService().go('templateslist');
                            // alert('Call APIS');
                        }}
                    />
                    </LeHPanel>
                </LeVPanel>
            </ReactMainContainer>
        );
    }
}
