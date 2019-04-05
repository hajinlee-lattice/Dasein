import React, { Component, react2angular } from "common/react-vendor";
import { store } from 'store';
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
    }

    handleChange = () => {
        const state = store.getState()['playbook'];
        this.setState({});
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
    .component('playbookOverview', {
        template: `<playbook-overview-react play="$ctrl.play" connections="$ctrl.connections"></playbook-overview-react>`,
        bindings: {
            play: '<',
            connections: '<'
        }
    })
    .component(
        "playbookOverviewReact",
        react2angular(OverviewComponent, ['play', 'connections'], ['$state', '$stateParams'])
    );