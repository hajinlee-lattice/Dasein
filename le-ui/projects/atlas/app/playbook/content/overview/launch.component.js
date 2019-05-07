import React, { Component, react2angular } from "common/react-vendor";
import Aux from 'common/widgets/hoc/_Aux';
import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
import './launch.component.scss';
import { actions as modalActions } from 'common/widgets/modal/le-modal.redux';
import { actions, reducer } from '../../playbook.redux';
import { store, injectAsyncReducer } from 'store';
import { LARGE_SIZE, MEDIUM_SIZE } from "common/widgets/modal/le-modal.utils";
import LeVPanel from "common/widgets/container/le-v-panel";
import LeHPanel from "common/widgets/container/le-h-panel";
import GridLayout from 'common/widgets/container/grid-layout.component';
import LeCard from 'common/widgets/container/card/le-card';
import LeCardImg from 'common/widgets/container/card/le-card-img';
import LeCardBody from 'common/widgets/container/card/le-card-body';
import LeButton from "common/widgets/buttons/le-button";
import { debounce } from 'lodash';
import {
  LEFT,
  CENTER,
  SPACEAROUND,
  SPACEBETWEEN,
  SPACEEVEN
} from "common/widgets/container/le-alignments";

/**
 * need time utility
 * need momentjs
 */ 

 function debounceEventHandler(...args) {
     const debounced = _.debounce(...args)
     return function(e) {
         e.persist()
         return debounced(e)
     }
 }

class LaunchComponent extends Component {
    constructor(props) {
        super(props);

        this.state = {
            refresh: false,
            play: props.play,
            ratings: null,
            selectedBuckets: [],
            recommendationCounts: {},
            unscored: false,
            limitRecommendations: false,
            limitRecommendationsAmount: null
        };

    }

    componentDidMount() {
        let playstore = store.getState()['playbook'];

        if(!playstore.ratings) {
            actions.fetchRatings([playstore.play.ratingEngine.id], false);
        } else {
            this.setState({ratings: playstore.ratings});
        }

        this.unsubscribe = store.subscribe(this.handleChange);
    }

    componentWillUnmount() {
      this.unsubscribe();
    }

    handleChange = () => {
        const state = store.getState()['playbook'];
        this.setState(state);
    }

    bucketClick = (bucket, coverage, play) => {
        return false;
        var selectedBuckets = this.state.selectedBuckets,
            index = selectedBuckets.indexOf(bucket.bucket);

        if (index > -1) {
            selectedBuckets.splice( index, 1 );
        } else {
            selectedBuckets.push( bucket.bucket );
        }

        this.state.selectedBuckets = selectedBuckets;
        this.setState(this.state);
        //console.log(this.state, selectedBuckets, this.state.selectedBuckets);

        //PlaybookWizardStore.setBucketsToLaunch(vm.selectedBuckets);
        //updatePage();
        this.state.recommendationCounts = this.makeRecommendationCounts(coverage, play);
        this.setState(this.state);
    }

    updateTopNCount() {
        var vm = {}; // placeholder
        vm.topNCount = Math.floor(vm.topNCount);
        if (vm.topNClicked && (vm.topNCount <= 0 || vm.topNCount == null)){
            vm.showError = true;
        } else {
            vm.showError = false;
            vm.topNClicked ? PlaybookWizardStore.setTopNCount(vm.topNCount) : PlaybookWizardStore.setTopNCount(null);
        }
    }

    makeRecommendationCounts(coverage, play, launchUnscored) {
        console.log('makeRecommendationCounts()');
        var vm = this; // placeholder
        var launchUnscored = this.state.unscored; // bool that should come from the include unscored checkbox
        var topNClicked  = false; //bool from Limit to only x recommentations
        var topNCount = play.launchHistory.mostRecentLaunch.topNCount; // can be changed by updateTopNCount()

        if(!coverage || !coverage.bucketCoverageCounts) {
            vm.recommendationCounts = null;
            return false;
        }
        var sections = {
                total: 0,
                selected: 0,
                suppressed: 0,
                launched: 0,
                contacts: 0
            },
            buckets = {};

        sections.total = play.targetSegment.accounts;

        var _contacts = 0;
        for(var i in vm.selectedBuckets) {
            var bucket = vm.selectedBuckets[i],
                count = coverage.bucketCoverageCounts.find(function(value) {
                    return value.bucket === bucket;
                });

            count = count || {count: 0};
            sections.selected += parseInt(count.count);
            _contacts = _contacts + parseInt(count.contactCount || 0);
        }

        sections.selected = launchUnscored ? coverage.unscoredAccountCount + sections.selected : sections.selected;

        if (topNClicked && topNCount <= sections.selected){
            sections.launched = topNCount <= 0 ? 0 : topNCount;
        } else {
            sections.launched = sections.selected;
        }

        sections.suppressed = sections.total >= sections.launched ? sections.total - sections.launched : sections.total;

        sections.contacts = _contacts + (launchUnscored ? (coverage.unscoredContactCount ? coverage.unscoredContactCount  : 0) : 0); 
        
        return sections;
    }

    makeBuckets(coverage, play) {
        console.log('makeBuckets()');
        var buckets = [],
            total = 0,
            vm = this;

        if(coverage && coverage.bucketCoverageCounts.length) {
            coverage.bucketCoverageCounts.forEach(function(bucket) {
                total = total + parseInt(bucket.count);
            });
            coverage.bucketCoverageCounts.forEach(function(bucket) {
                let percent = Math.floor((bucket.count / total) * 100),
                    selected = (vm.state.selectedBuckets.indexOf(bucket.bucket) >= 0);

                buckets.push(
                    <span className={`${(selected ? 'selected' : '')}`} onClick={() => {vm.bucketClick(bucket, coverage, play) }}>
                        <h3>{bucket.bucket}</h3>
                        {bucket.count} ({percent}%)
                    </span>
                );
            });
        }
        return buckets;
    }

    clickUnscored = (e) => {
        this.state.unscored = e.target.checked;
        this.setState(this.state);
    }

    clickContactInfo = (e) => {
        console.log(e.target.checked);
    }

    clickRequireAccountId = (e) => {
        console.log(e.target.checked);
    }

    clickLimitRecommendations = (e) => {
        this.state.limitRecommendations = e.target.checked;
        this.setState(this.state);
    }

    enterLimitRecommendationsAmount = (e) => {
        this.state.limitRecommendationsAmount = e.target.value;
        this.setState(this.state);
    }

    clickEmail = (e) => {
        console.log(e.target.checked);
    }

    render() {
        if(this.state.ratings) {
            var play = this.state.play,
                accountsCoverage = this.state.ratings,
                engineId = (accountsCoverage && accountsCoverage.ratingEngineIdCoverageMap && accountsCoverage.ratingEngineIdCoverageMap[Object.keys(accountsCoverage.ratingEngineIdCoverageMap)[0]] ? Object.keys(accountsCoverage.ratingEngineIdCoverageMap)[0] : ''),
                coverage = (engineId && accountsCoverage.ratingEngineIdCoverageMap[engineId] ? accountsCoverage.ratingEngineIdCoverageMap[engineId] : {}),
                unscoredAccountCountPercent = Math.floor((coverage.unscoredAccountCount / (coverage.unscoredAccountCount + coverage.accountCount)) * 100) || 0,
                bucketsToLaunch = play.launchHistory.mostRecentLaunch.bucketsToLaunch,
                selectedBuckets = this.selectedBuckets,
                numAccounts = coverage.unscoredAccountCount + coverage.accountCount;

            var recommendationCounts = this.makeRecommendationCounts(coverage, play);

            if(coverage){
                coverage.bucketCoverageCounts.forEach(function(bucket){
                    if(bucketsToLaunch == null) {
                        //selectedBuckets.push(bucket.bucket);
                    } else if(bucketsToLaunch.length && bucketsToLaunch.indexOf(bucket.bucket) !== -1) {
                        //selectedBuckets.push(bucket.bucket);
                    }

                    bucket.percentage = bucket.count / numAccounts;               
                });
                //PlaybookWizardStore.setBucketsToLaunch(vm.selectedBuckets);
            }

            return (
                <Aux>
                    <LeVPanel className={'campaign-launch'} hstretch={true}>
                        <div>
                            <h2>Model Ratings</h2>
                            <LeHPanel hstretch={true} halignment={LEFT} valignment={CENTER} className={'rating-buckets'}>
                                {this.makeBuckets(coverage, play)}
                            </LeHPanel>
                            <input id="unscored" type="checkbox" onChange={this.clickUnscored} /> 
                            <label for="unscored">
                                Include the <strong>{coverage.unscoredAccountCount.toLocaleString()} ({unscoredAccountCountPercent}%)</strong> Unscored Accounts
                            </label>
                        </div>
                        <div>
                            <h2>Account Options</h2>
                            <ul>
                                <li>
                                    <input id="contactInfo" onChange={this.clickContactInfo} type="checkbox" /> 
                                    <label for="contactInfo">Must have contact info</label>
                                </li>
                                <li>
                                    <input id="requireAccountId" onChange={this.clickRequireAccountId} type="checkbox" /> 
                                    <label for="requireAccountId">Must have account ID</label>
                                </li>
                            </ul>
                        </div>
                        <div>
                            <h2>Recommendations to be Launched: {recommendationCounts.launched}</h2>
                            <ul>
                                <li>
                                    <input id="limitRecommendations" onChange={this.clickLimitRecommendations} type="checkbox" /> 
                                    <label for="limitRecommendations"> 
                                        Limit to only <input id="limitRecommendationsAmount" type="number" min="1" class={`${!this.state.limitRecommendationsAmount ? 'empty' : ''} ${this.state.limitRecommendations ? 'required' : ''}`} required={this.state.limitRecommendations} onChange={debounceEventHandler(this.enterLimitRecommendationsAmount, 200)} /> recommendations
                                    </label>
                                </li>
                                <li>
                                    <input id="email" onChange={this.clickEmail} type="checkbox" /> <label for="email">Email me when new data is available</label>
                                </li>
                            </ul>
                        </div>
                        <div className={'launch-buttons'}>
                            <ul>
                                <li>
                                    <LeButton
                                        name="cancel"
                                        disabled={false}
                                        config={{
                                            label: "Cancel",
                                            classNames: "white-button"
                                        }}
                                        callback={() => { this.props.closeFn(); this.setState({refresh: true}) }} />
                                </li>
                                <li>
                                    <LeButton
                                        name="launchlater"
                                        disabled={false}
                                        config={{
                                            label: "Launch Later",
                                            classNames: "white-button"
                                        }}
                                        callback={() => { modalActions.closeModal(store); this.setState({refresh: true}) }} />
                                </li>
                                <li>
                                    <LeButton
                                        name="launchnow"
                                        disabled={false}
                                        config={{
                                            label: "Launch Now",
                                            classNames: "white-button"
                                        }}
                                        callback={() => { modalActions.closeModal(store); this.setState({refresh: true}) }} />
                                </li>
                                <li>
                                    <LeButton
                                        name="launchautomatically"
                                        disabled={false}
                                        config={{
                                            label: "Launch Automatically",
                                            classNames: "blue-button"
                                        }}
                                        callback={() => { modalActions.closeModal(store); this.setState({refresh: true}) }} />
                                </li>
                            </ul>
                        </div>
                    </LeVPanel>
                </Aux>
            );
        } else {
            return (
                <Aux>
                    loading...
                </Aux>
            );
        }
    }
}
export default LaunchComponent;