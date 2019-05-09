import React, { Component } from "common/react-vendor";
//import ReactRouter from 'atlas/react/router';
import Aux from 'common/widgets/hoc/_Aux';
import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
import { Observable } from "common/network.vendor";
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
import './launch.component.scss';

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
            play: props.play,
            ratings: null,
            coverage: null,
            selectedBuckets: [],
            recommendationCounts: {},
            unscored: false,
            limitRecommendations: false,
            limitRecommendationsAmount: null,
            excludeItemsWithoutSalesforceId: false,
            launchAccountsCoverage: null
        };

    }

    componentDidMount() {
        let playstore = store.getState()['playbook'];

        if(!playstore.ratings) {
            actions.fetchRatings([playstore.play.ratingEngine.id], false);
        } else {
            this.setState({ratings: playstore.ratings});
        }

        var vm = this;
        playstore.playbookWizardStore.launchAccountsCoverage(this.state.play.name, true).then(function(response) {
            vm.state.launchAccountsCoverage = response;
            vm.setState(vm.state);
        });

        //console.log(this.PlaybookWizardStore.launchAccountsCoverage(this.state.play.name));

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
        var selectedBuckets = this.state.selectedBuckets,
            index = selectedBuckets.indexOf(bucket.bucket);

        if (index > -1) {
            selectedBuckets.splice( index, 1 );
        } else {
            selectedBuckets.push( bucket.bucket );
        }

        this.state.selectedBuckets = selectedBuckets;
        this.setState(this.state);
    }

    makeRecommendationCounts(coverage, play) {
        var vm = this; // placeholder

        if(!coverage || !coverage.bucketCoverageCounts) {
            this.state.recommendationCounts = null;
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
        for(var i in vm.state.selectedBuckets) {
            var bucket = vm.state.selectedBuckets[i],
                count = coverage.bucketCoverageCounts.find(function(value) {
                    return value.bucket === bucket;
                });

            count = count || {count: 0};
            sections.selected += parseInt(count.count);
            _contacts = _contacts + parseInt(count.contactCount || 0);
        }

        sections.selected = this.state.unscored ? coverage.unscoredAccountCount + sections.selected : sections.selected;

        if (this.state.limitRecommendations && this.state.limitRecommendationsAmount <= sections.selected){
            sections.launched = this.state.limitRecommendationsAmount <= 0 ? 0 : this.state.limitRecommendationsAmount;
        } else {
            sections.launched = sections.selected;
        }

        sections.suppressed = sections.total >= sections.launched ? sections.total - sections.launched : sections.total;

        sections.contacts = _contacts + (this.state.unscored ? (coverage.unscoredContactCount ? coverage.unscoredContactCount  : 0) : 0);

        return sections;
    }

    makeBuckets(coverage, play, buckets) {
        var _buckets = [],
            total = 0,
            vm = this,
            coverageBuckets = (coverage && coverage.bucketCoverageCounts && coverage.bucketCoverageCounts.length ? coverage.bucketCoverageCounts : []),
            buckets = buckets || coverageBuckets;

        if(buckets) {
            buckets.forEach(function(bucket) {
                total = total + parseInt(bucket.count);
            });
            buckets.forEach(function(bucket) {
                let percent = Math.floor((bucket.count / total) * 100) || 0,
                    selected = (vm.state.selectedBuckets.indexOf(bucket.bucket) >= 0);

                _buckets.push(
                    <span className={`${(selected ? 'selected' : '')}`} disabled={(bucket.count <= 0)} onClick={() => { if(bucket.count) { vm.bucketClick(bucket, coverage, play) } }}>
                        <h3>{bucket.bucket}</h3>
                        {bucket.count} ({percent}%)
                    </span>
                );
            });
        }
        return _buckets;
    }

    makeBucketList(play, coverage, unscoredAccountCountPercent) {
        /**
         * If no buckets this should produce 5 default buckets
         */
        let _noBuckets = [{
            bucket: 'A',
            count: 0,
        },{
            bucket: 'B',
            count: 0,
        },{
            bucket: 'C',
            count: 0,
        },{
            bucket: 'D',
            count: 0,
        },{
            bucket: 'E',
            count: 0,
        },{
            bucket: 'F',
            count: 0,
        }],
        noBuckets = (coverage && coverage.bucketCoverageCounts && coverage.bucketCoverageCounts.length ? null : _noBuckets);

        console.log(coverage, coverage.bucketCoverageCounts, coverage.bucketCoverageCounts.length, noBuckets);

        if(coverage) {
            return (
                <Aux>
                    <h2>Model Ratings</h2>
                    <LeHPanel hstretch={true} halignment={LEFT} valignment={CENTER} className={'rating-buckets'}>
                        {this.makeBuckets(coverage, play, noBuckets)}
                    </LeHPanel>
                    <input id="unscored" type="checkbox" onChange={this.clickUnscored} /> 
                    <label for="unscored">
                        Include the <strong>{(coverage && coverage.unscoredAccountCount ? coverage.unscoredAccountCount.toLocaleString() : 0)} ({unscoredAccountCountPercent}%)</strong> Unscored Accounts
                    </label>
                </Aux>
            );
        }
    }

    launch = (play, connection, opts) => {
        var opts = opts || {},
            play = opts.play || store.getState().playbook.play,
            ratings = store.getState().playbook.ratings,
            launchObj = opts.launchObj || {
                bucketsToLaunch: this.state.selectedBuckets,
                destinationOrgId: connection.orgId,
                destinationSysType: connection.externalSystemType,
                destinationAccountId: connection.orgName,
                topNCount: (this.state.limitRecommendations ? this.state.limitRecommendationsAmount : ''),
                launchUnscored: this.state.unscored,
                excludeItemsWithoutSalesforceId: this.state.excludeItemsWithoutSalesforceId
                // audienceName: PlaybookWizardStore.getAudienceName(), //?
                // audienceId: PlaybookWizardStore.getAudienceId(), //?
                // folderName: PlaybookWizardStore.getMarketoProgramName(), //?
            },
            save = opts.save || false,
            lastIncompleteLaunchId = (play.launchHistory.lastIncompleteLaunch ? play.launchHistory.lastIncompleteLaunch.launchId : ''),
            lastIncompleteLaunch = opts.lastIncompleteLaunch || null;

        if(play) {
            if(lastIncompleteLaunch) {
                actions.saveLaunch(play.name, {
                    engineId: opts.engineId,
                    launch_id: lastIncompleteLaunch.launchId,
                    launchObj: Object.assign({},lastIncompleteLaunch, launchObj),
                    save: true
                });
            } else if(lastIncompleteLaunchId) {
                actions.savePlayLaunch(play.name, {
                    engineId: opts.engineId,
                    launch_id: lastIncompleteLaunchId,
                    launchObj: Object.assign({}, PlaybookWizardStore.currentPlay.launchHistory.lastIncompleteLaunch, launchObj),
                    save: save
                });
            } else {
                actions.savePlayLaunch(play.name, {
                    engineId: opts.engineId,
                    launchObj: launchObj,
                    save: save
                });
            }
        }
    }

    clickUnscored = (e) => {
        this.state.unscored = e.target.checked;
        this.setState(this.state);
    }

    clickContactInfo = (e) => {
        console.log(e.target.checked);
        // not currenlty supported
        // <li>
        //     <input id="contactInfo" onChange={this.clickContactInfo} type="checkbox" /> 
        //     <label for="contactInfo">Must have contact info</label>
        // </li>
    }

    clickRequireAccountId = (e) => {
        this.state.excludeItemsWithoutSalesforceId = e.target.checked;
        this.setState(this.state);
    }

    clickLimitRecommendations = (e) => {
        this.state.limitRecommendations = e.target.checked;
        this.setState(this.state);
    }

    enterLimitRecommendationsAmount = (e) => {
        this.state.limitRecommendationsAmount = e.target.value;
        this.state.limitRecommendations = true;
        this.setState(this.state);
    }

    clickEmail = (e) => {
        console.log(e.target.checked);
        // not currenlty supported
        // <li>
        //     <input id="email" onChange={this.clickEmail} type="checkbox" /> <label for="email">Email me when new data is available</label>
        // </li>
    }

    render() {
        if(this.state.launchAccountsCoverage) {
            var play = this.state.play,
                connection = this.props.connection,
                bucketsToLaunch = play.launchHistory.mostRecentLaunch.bucketsToLaunch,
                accountsCoverage = this.state.launchAccountsCoverage, //this.state.ratings,
                coverageType = (accountsCoverage.ratingModelsCoverageMap ? 'ratingModelsCoverageMap' : 'ratingEngineIdCoverageMap'),
                engineId,
                coverage;

            if(coverageType === 'ratingModelsCoverageMap') {
                engineId = (accountsCoverage && accountsCoverage.engineId ? accountsCoverage.engineId : '');
                coverage = (engineId && accountsCoverage[coverageType] ? accountsCoverage[coverageType] : {});
            } else {
                engineId = (accountsCoverage && accountsCoverage[coverageType] && accountsCoverage[coverageType][Object.keys(accountsCoverage[coverageType])[0]] ? Object.keys(accountsCoverage[coverageType])[0] : '');
                coverage = (engineId && accountsCoverage[coverageType][engineId] ? accountsCoverage[coverageType][engineId] : {});
            }

            var unscoredAccountCountPercent = Math.floor((coverage.unscoredAccountCount / (coverage.unscoredAccountCount + coverage.accountCount)) * 100) || 0,
                selectedBuckets = this.selectedBuckets,
                numAccounts = coverage.unscoredAccountCount + coverage.accountCount,
                recommendationCounts = this.makeRecommendationCounts(coverage, play),
                canLaunch = recommendationCounts.launched;

            // let state = this.state;
            // state.coverage = coverage;
            // this.setState(state);

            if(coverage && coverage.bucketCoverageCounts){
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
                <LeVPanel className={'campaign-launch'} hstretch={true}>
                    <div className={'launch-section model-ratings'}>
                        {this.makeBucketList(play, coverage, unscoredAccountCountPercent)}
                    </div>
                    <div className={'launch-section account-options'}>
                        <h2>Account Options</h2>
                        <ul>
                            <li>
                                <input id="requireAccountId" onChange={this.clickRequireAccountId} type="checkbox" /> 
                                <label for="requireAccountId">Must have account ID</label>
                            </li>
                        </ul>
                    </div>
                    <div className={'launch-section recommendations'}>
                        <h2>Recommendations to be Launched: {recommendationCounts.launched}</h2>
                        <ul>
                            <li>
                                <input id="limitRecommendations" checked={this.state.limitRecommendations} onChange={this.clickLimitRecommendations} type="checkbox" /> 
                                <label for="limitRecommendations"> 
                                    Limit to only <input id="limitRecommendationsAmount" type="number" min="1" class={`${!this.state.limitRecommendationsAmount ? 'empty' : ''} ${this.state.limitRecommendations ? 'required' : ''}`} required={this.state.limitRecommendations} onChange={debounceEventHandler(this.enterLimitRecommendationsAmount, 200)} /> recommendations
                                </label>
                            </li>
                        </ul>
                    </div>
                    <div className={'launch-section launch-buttons'}>
                        <ul>
                            <li>
                                <LeButton
                                    name="cancel"
                                    disabled={false}
                                    config={{
                                        label: "Cancel",
                                        classNames: "white-button"
                                    }}
                                    callback={() => { this.props.closeFn(); }} />
                            </li>
                            <li>
                                <LeButton
                                    name="launchlater"
                                    disabled={!canLaunch}
                                    config={{
                                        label: "Launch Later",
                                        classNames: "white-button"
                                    }}
                                    callback={() => { 
                                        this.launch(play, connection, {
                                            engineId: engineId, 
                                            lastIncompleteLaunch: play.launchHistory.lastIncompleteLaunch,
                                            save: false
                                        });
                                    }} />
                            </li>
                            <li>
                                <LeButton
                                    name="launchnow"
                                    disabled={!canLaunch}
                                    config={{
                                        label: "Launch Now",
                                        classNames: "white-button"
                                    }}
                                    callback={() => { 
                                        this.launch(play, connection, {
                                            engineId: engineId, 
                                            lastIncompleteLaunch: play.launchHistory.lastIncompleteLaunch,
                                            save: true
                                        }); 
                                    }} />
                            </li>
                            <li>
                                <LeButton
                                    name="launchautomatically"
                                    disabled={true}
                                    config={{
                                        label: "Launch Automatically",
                                        classNames: "blue-button"
                                    }}
                                    callback={() => {
                                        console.log('Launch Automatically Clicked');
                                    }} />
                            </li>
                        </ul>
                    </div>
                </LeVPanel>
            );
        } else {
            return (
                <Aux>
                    <p>Loading...</p>
                </Aux>
            );
        }
    }
}
export default LaunchComponent;