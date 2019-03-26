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
        console.log(this.props, this.state);
    }

    componentDidMount() {
        this.unsubscribe = store.subscribe(this.handleChange);
    }

    componentWillUnmount() {
        this.unsubscribe();
    }

    handleChange = () => {
        const state = store.getState()['playbook'];
        this.setState(state);
    }

    render() {
        console.log(this.state);
        return (
            <div className="main-panel">
                <LeHPanel halignment={SPACEAROUND}>
                    <MainComponent />
                    <RatingsComponent />
                </LeHPanel>
                <SystemsComponent />
            </div>
        );
    }
}

angular
    .module("lp.playbook.overview", [])
    .component(
        "playbookOverview",
        react2angular(OverviewComponent, ['Play'], ['$state','$stateParams'])
    );