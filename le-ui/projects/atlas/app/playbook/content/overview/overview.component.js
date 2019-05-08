import React, { Component, react2angular } from "common/react-vendor";
import { store, injectAsyncReducer } from 'store';
import { actions, reducer } from '../../playbook.redux';
import './overview.component.scss';
import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
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

export default class OverviewComponent extends Component {
    constructor(props) {
        super(props);
        this.state = {
            play: props.play,
            connections: null
        };
    }

    componentDidMount() {
        // injectAsyncReducer(store, 'playbook', reducer);
        let playstore = store.getState()['playbook'];

        //actions.fetchPlay(playstore.play.name);
        actions.fetchConnections(playstore.play.name);

        this.unsubscribe = store.subscribe(this.handleChange);
    }

    componentWillUnmount() {
      this.unsubscribe();
    }

    handleChange = () => {
        const state = store.getState()['playbook'];
        this.setState(state);
        //console.log('handleChange', state);
    }

    makeTalkingpoints(play) {
        if(!play.talkingPoints || (play.talkingPoints && !play.talkingPoints.length)) {
            return (
                <div className="talking-points">
                    <a ui-sref="home.playbook.dashboard.insights({play_name: vm.play.name})">Create Talking Points</a>
                    No talking points
                </div>
            );
        } else {
            return (
                <div className="talking-points">
                    <a ui-sref="home.playbook.dashboard.insights({play_name: vm.play.name})">Edit Talking points</a>
                    {play.talkingPoints.length} talking points have been created
                </div>
            );
        }
    }

    render() {
        if (this.state.play) {
            return (
                <ReactMainContainer className={'container playbook-overview'}>
                    <LeHPanel hstretch={"true"} vstretch={"true"}>
                        <div class="systems-component">
                            <SystemsComponent play={this.state.play} connections={this.state.connections} />
                        </div>
                        <div class="main-component">
                            <GridLayout classNames="overview-grid extend">
                                <span>
                                    <RatingsComponent play={this.state.play} />
                                </span>
                                <span>
                                    <h2>Talking Points</h2>
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
                    loading...
                </ReactMainContainer>
            );
        }
    }
}

// angular
//     .module("lp.playbook.overview", [])
//     .component(
//         "playbookOverview",
//         react2angular(OverviewComponent, ['play', 'connections'], ['$state', '$stateParams'])
//     );