import React, { Component, react2angular } from "common/react-vendor";
import { store, injectAsyncReducer } from 'store';
import { actions, reducer } from '../../playbook.redux';
import './overview.component.scss';
import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
import LeVPanel from "common/widgets/container/le-v-panel";
import LeHPanel from "common/widgets/container/le-h-panel";
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

export class OverviewComponent extends Component {
    constructor(props) {
        super(props);
        this.state = {
            play: props.play,
            connections: props.connections
        };
    }

    componentDidMount() {
        this.unsubscribe = store.subscribe(this.handleChange);

        //injectAsyncReducer(store, 'playbook', reducer);
        //let playstore = store.getState()['playbook'];
        //actions.fetchPlay(this.props.playname);
        //actions.fetchConnections(this.props.playname);

        console.log('props', this.props);
    }

    componentWillUnmount() {
        this.unsubscribe();
    }

    handleChange = () => {
        const state = store.getState()['playbook'];
        this.setState(state);
        console.log('handleChange', state);
    }

    render() {
        if (this.state.play) {
            return (
                <div className="playbook-overview">
                    <LeHPanel halignment={SPACEAROUND}>
                        <ul>
                            <li>
                                {this.state.play.displayName}
                            </li>
                            <li>
                                Segment: {this.state.play.targetSegment.display_name}
                            </li>
                            <li>
                                Model: {this.state.play.ratingEngine.displayName}
                            </li>
                            <li>
                                Available Targets: {this.state.play.targetSegment.accounts} Accounts | {this.state.play.targetSegment.contacts} Contacts
                            </li>
                        </ul>
                        <ul>
                            <li>
                                Campaign Type: {this.state.play.playType.displayName}
                            </li>
                            <li>
                                Talking Point: {this.state.play.talkingPoints.length} talking points
                            </li>
                            <li>
                                Description: {this.state.play.description}
                            </li>
                        </ul>
                    </LeHPanel>
                    <LeVPanel hstretch={"true"}>
                        <SystemsComponent play={this.state.play} />
                    </LeVPanel>
                </div>
            );
        } else {
            return (
                <div className="playbook-overview">
                    loading...
                </div>
            );
        }
    }
}

angular
    .module("lp.playbook.overview", [])
    .component(
        "playbookOverview",
        react2angular(OverviewComponent, ['play', 'connections'], ['$state', '$stateParams'])
    );