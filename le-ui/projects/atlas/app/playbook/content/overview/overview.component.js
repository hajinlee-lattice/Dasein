import React, { Component, react2angular } from "common/react-vendor";
import { actions, reducer } from '../../playbook.redux';
import { store } from 'common/app/store';
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
        this.state = {play: null};
    }

    componentDidMount() {
        let play = this.props.OverviewService.getPlay();
        this.setState({play: play});
        // this.unsubscribe = store.subscribe(this.handleChange);
    }

    componentWillUnmount() {
        // this.unsubscribe();
    }

    handleChange = () => {
        // const state = store.getState()['playbook'];
        // this.setState({});
    }

    render() {
        if(this.state.play) {
            return (
                <div className="playbook-overview">
                    <LeHPanel halignment={SPACEAROUND}>
                        <MainComponent play={this.state.play} />
                        <RatingsComponent play={this.state.play} />
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
    .service('OverviewService', function () {
        this.Play;
        this.getPlay = () => {
            return this.Play;
        }
        this.setPlay = (Play) =>{
            this.Play = Play;
        }

    })
    .component(
        "playbookOverview",
        react2angular(OverviewComponent, ['Play'], ['$state','$stateParams', 'OverviewService'])
    );