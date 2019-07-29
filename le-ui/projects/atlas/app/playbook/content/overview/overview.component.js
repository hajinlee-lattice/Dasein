import React, { Component } from "common/react-vendor";
import { store } from 'store';
import { actions } from '../../playbook.redux';
//import ReactRouter from 'atlas/react/router';
import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
import NgState from "atlas/ng-state";
import LeVPanel from "common/widgets/container/le-v-panel";
import LeHPanel from "common/widgets/container/le-h-panel";
import GridLayout from 'common/widgets/container/grid-layout.component';
import ReactMainContainer from "atlas/react/react-main-container";
import MainComponent from "./main.component";
import RatingsComponent from "./ratings.component";
import SystemsComponent from "./systems.component";
import {
    LEFT,
    RIGHT,
    TOP,
    BOTTOM,
    CENTER,
    SPACEAROUND,
    SPACEBETWEEN,
    SPACEEVEN
} from "common/widgets/container/le-alignments";
import './overview.component.scss';

export default class OverviewComponent extends Component {
    constructor(props) {
        super(props);
        //this.PlaybookWizardStore = ReactRouter.getRouter().ngservices.PlaybookWizardStore;
        this.state = {
            play: props.play,
            connections: null,
            talkingPointsText: ''
        };
    }

    componentDidMount() {
        let playstore = store.getState()['playbook'];
        
        actions.fetchConnections(playstore.play.name);
        actions.addPlaybookWizardStore(this.props.PlaybookWizardStore);

        this.unsubscribe = store.subscribe(this.handleChange);
    }

    componentWillUnmount() {
        actions.reset();
        this.unsubscribe();
    }

    handleChange = () => {
        const state = store.getState()['playbook'];
        this.setState(state);
        //console.log('handleChange', state);
    }

    goto = (route, params) => {
        //ngState.getAngularState().go(route, params)
    }

    makeTalkingpointsText(play) {
        if(!play.talkingPoints || (play.talkingPoints && !play.talkingPoints.length)) {
            return 'Create Talking Points';
        } else {
            return 'Edit Talking Points';
        }
    }

    makeTalkingpoints(play) {
        if(!play.talkingPoints || (play.talkingPoints && !play.talkingPoints.length)) {
            return (
                <div className="talking-points-count">
                    No talking points
                </div>
            );
        } else {
            return (
                <div className="talking-points-count">
                    {play.talkingPoints.length} talking points have been created
                </div>
            );
        }
    }

    launchHistoryLink(play) {
        //if(play.launchHistory.mostRecentLaunch && play.launchHistory.mostRecentLaunch.launchState && ['Launching','Launched','Failed'].indexOf(play.launchHistory.mostRecentLaunch.launchState) !== -1 || (play.launchHistory.lastCompletedLaunch && play.launchHistory.lastCompletedLaunch.launchState)) {
            return(
                <a href="javascript:void(0);" onClick={() => {
                    NgState.getAngularState().go('home.playbook.dashboard.launchhistory', {play_name: play.name}); 
                }}>Launch History</a>
            );
        //}
    }
    render() {
        if (this.state.play) {
            return (
                <ReactMainContainer className={'container playbook-overview show-spinner'}>
                    <div className="overview-header">
                        <h1>
                            Campaign Dashboard
                        </h1>
                        <div className="launch-history">
                            {this.launchHistoryLink(this.state.play)}
                        </div>
                    </div>
                    <LeHPanel hstretch={"true"} vstretch={"true"} className="components">
                        <div class="systems-component">
                            <SystemsComponent play={this.state.play} connections={this.state.connections} />
                        </div>
                        <div class="main-component">
                            <GridLayout classNames="overview-grid extend">
                                <span>
                                    <RatingsComponent play={this.state.play} />
                                </span>
                                <span>
                                    <h2 className="panel-label talking-points-label">
                                        <strong>SFDC Talking Points</strong>
                                        <a href="javascript:void(0);" onClick={() => {
                                            NgState.getAngularState().go('home.playbook.dashboard.insights', {play_name: this.state.play.name}); 
                                        }}>{this.makeTalkingpointsText(this.state.play)}</a>
                                    </h2>
                                    {this.makeTalkingpoints(this.state.play)}
                                </span>
                            </GridLayout>
                        </div>
                    </LeHPanel>
                </ReactMainContainer>
            );
        } else {
            return (
                <ReactMainContainer className={'container playbook-overview'}>
                    <div className="loader"></div>
                </ReactMainContainer>
            );
        }
    }
}