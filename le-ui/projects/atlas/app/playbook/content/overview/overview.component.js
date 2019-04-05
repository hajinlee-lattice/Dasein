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
        this.state = { play: null };
    }

    componentDidMount() {
        this.unsubscribe = store.subscribe(this.handleChange);

        let play = this.props.OverviewService.getPlay();

        this.setState({
            play: play
        });

        //actions.fetchConnections(play.name);

    }

    handleChange = () => {
        const state = store.getState()['playbook'];
        this.setState({});

        console.log('handleChange', state);
    }

    render() {
        if(this.state.play) {
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
    .service('OverviewService', function () {
        this.Play;
        this.getPlay = () => {
            return this.Play;
        }
        this.setPlay = (Play) =>{
            this.Play = Play;
        }
    })
    .component('playbookOverview', {
        template: `<playbookOverviewReact></playbookOverviewReact>`,
        binding: {
            Play: '<'
        },
        controllerAs: 'vm',
        controller: function ($state, OverviewService, $ngRedux) {
            'ngInject';

            let vm = this;

            vm.OverviewService = OverviewService;
            vm.ReduxStore = $state.get('home.playbook.overview').data.store;
console.log('controller', this, $state.get('home.playbook.overview').data.store);
            vm.$onInit = function() {
                $ngRedux.subscribe(state => {
                    console.log('-!- Redux store has changed', vm.redux.store);
                    vm.ReduxStore.fetchConnections(this.Play.name);
                });
            }
        }
    })
    .component(
        "playbookOverviewReact",
        react2angular(OverviewComponent, [], ['$state','$stateParams', 'OverviewService'])
    );