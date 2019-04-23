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
            connections: [] //props.connections
        };
    }

    componentDidMount() {
        injectAsyncReducer(store, 'playbook', reducer);
        // let playstore = store.getState()['playbook'];
        // actions.fetchPlay(this.props.playname);
        // actions.fetchConnections(this.props.playname);
        
        this.unsubscribe = store.subscribe(this.handleChange);

        console.log('props', this.props);
        // actions.fetchConnections(this.state.play.playname);
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
        return (
            <ReactMainContainer>
                <p>hello</p>
            </ReactMainContainer>
        );
        if (this.state.play) {
                //<div className="playbook-overview">
            return (
                <section id="main-content" class="container playbook-overview">
                    <div class="overview-grid-container">
                        {this.state.play.displayName}
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
                    <SystemsComponent play={this.state.play} connections={this.state.connections} />
                </section>
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

// angular
//     .module("lp.playbook.overview", [])
//     .component(
//         "playbookOverview",
//         react2angular(OverviewComponent, ['play', 'connections'], ['$state', '$stateParams'])
//     );