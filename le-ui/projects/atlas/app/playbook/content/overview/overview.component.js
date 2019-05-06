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
console.log('constructor');
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

    render() {
console.log('OverviewComponent');
        if (this.state.play) {
console.log('this.state.play');
            return (
                <ReactMainContainer className={'container playbook-overview'}>
                    <LeHPanel hstretch={"true"} vstretch={"true"}>
                        <div class="systems-component">
                            <SystemsComponent play={this.state.play} connections={this.state.connections} />
                        </div>
                        <div class="main-component">
                            <RatingsComponent play={this.state.play} />
                            <GridLayout classNames="overview-grid extend">
                                <span>
                                    Segment: {this.state.play.targetSegment.display_name}
                                </span>
                                <span>
                                    Campaign Type: {this.state.play.playType.displayName}
                                </span>
                                <span>
                                    Model: {this.state.play.ratingEngine.displayName}
                                </span>
                                <span>
                                    Talking Point: {this.state.play.talkingPoints.length} talking points
                                </span>
                                <span>
                                    Available Targets: {this.state.play.targetSegment.accounts} Accounts | {this.state.play.targetSegment.contacts} Contacts
                                </span>
                                <span>
                                    Description: {this.state.play.description}
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