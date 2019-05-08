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
            play: props.play,
            ratings: null,
            coverage: null,
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

    makeBuckets(coverage, play) {
        var buckets = [],
            total = 0,
            vm = this;

        if(coverage && coverage.bucketCoverageCounts && coverage.bucketCoverageCounts.length) {
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

    makeBucketList(play, coverage, unscoredAccountCountPercent) {
        if(coverage) {
            return (
                <Aux>
                    <h2>Model Ratings</h2>
                    <LeHPanel hstretch={true} halignment={LEFT} valignment={CENTER} className={'rating-buckets'}>
                        {this.makeBuckets(coverage, play)}
                    </LeHPanel>
                    <input id="unscored" type="checkbox" onChange={this.clickUnscored} /> 
                    <label for="unscored">
                        Include the <strong>{(coverage && coverage.unscoredAccountCount ? coverage.unscoredAccountCount.toLocaleString() : 0)} ({unscoredAccountCountPercent}%)</strong> Unscored Accounts
                    </label>
                </Aux>
            );
        }
    }

    nextSaveLaunch = (play, connection, opts) => {
        console.log(play, connection, opts);
        console.group('nextSaveLaunch');
        // what are audience name and id and waht is folder name?
        // how to call fetch ratings from in here, or do I call it from react and then access the store (currently doing this)?
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
                excludeItemsWithoutSalesforceId: false
                // audienceName: PlaybookWizardStore.getAudienceName(), //?
                // audienceId: PlaybookWizardStore.getAudienceId(), //?
                // folderName: PlaybookWizardStore.getMarketoProgramName(), //?
            },
            saveOnly = opts.saveOnly || false,
            lastIncompleteLaunchId = (play.launchHistory.lastIncompleteLaunch ? play.launchHistory.lastIncompleteLaunch.launchId : ''),
            lastIncompleteLaunch = opts.lastIncompleteLaunch || null;


        if(play) {
            // do I need to do this?
            // if(play.ratingEngine){
            //     RatingsEngineStore.getRating(play.ratingEngine.id).then(function(result){
            //         PlaybookWizardStore.setRating(result);
            //     });
            // } else {
            //     var ratingEngine = PlaybookWizardStore.getSavedRating(); //?
            //     play.ratingEngine = ratingEngine;
            // }
            // launch saved play
            if(lastIncompleteLaunch) {
                // ow do I call saveLaunch?
                console.group('lastIncompleteLaunch');
                    console.log('lastIncompleteLaunch:', lastIncompleteLaunch);
                    console.log('saveLaunch()', 'save');
                    console.log('saveLaunch()', 'launch');
                console.groupEnd();
                // PlaybookWizardService.saveLaunch(PlaybookWizardStore.currentPlay.name, {
                //     launch_id: lastIncompleteLaunch.launchId,
                //     launchObj: Object.assign({},lastIncompleteLaunch, launchObj)
                // }).then(function(launch) {
                //     PlaybookWizardService.saveLaunch(PlaybookWizardStore.currentPlay.name, {
                //         launch_id: lastIncompleteLaunch.launchId,
                //         action: 'launch'
                //     }).then(function(saved) {
                //         $state.go('home.playbook.dashboard.launch_job', {play_name: play.name, applicationId: saved.applicationId});
                //     });
                // });
            } else if(lastIncompleteLaunchId) {
                // save play
                console.group('lastIncompleteLaunchId');
                    console.log('lastIncompleteLaunchId:', lastIncompleteLaunchId);
                    console.log('savePlay()?');
                    console.log('saveLaunch()', 'save');
                    if(!saveOnly) {
                        console.log('saveLaunch()', 'launch');
                    }
                console.groupEnd();
                // PlaybookWizardStore.savePlay(play).then(function(play) {
                //     // save launch
                //     PlaybookWizardService.saveLaunch(PlaybookWizardStore.currentPlay.name, {
                //         launch_id: lastIncompleteLaunchId,
                //         launchObj: Object.assign({}, PlaybookWizardStore.currentPlay.launchHistory.lastIncompleteLaunch, launchObj)
                //     }).then(function(saved) {
                //         if(!saveOnly) {
                //             // launch
                //             PlaybookWizardService.saveLaunch(play.name, {
                //                 launch_id: lastIncompleteLaunchId,
                //                 action: 'launch'
                //             }).then(function(launch) {
                //                 // after launch
                //                 $state.go('home.playbook.dashboard.launch_job', {play_name: play.name, applicationId: saved.applicationId});
                //             });
                //         } else {
                //             // saved but not launched
                //             $state.go('home.playbook');
                //         }
                //     });
                // });
            } else {
                console.group('else');
                    console.log('savePlay()?');
                    console.log('saveLaunch()', 'save');
                    if(!saveOnly) {
                        console.log('saveLaunch()', 'launch');
                    }
                console.groupEnd();

                // save play
                // PlaybookWizardStore.savePlay(play).then(function(play) {
                //     // get launchid
                //     PlaybookWizardService.saveLaunch(play.name, {
                //         launchObj: launchObj
                //     }).then(function(launch) {
                //         var launch = launch || {};
                //         // save launch
                //         if(launch && !saveOnly) {
                //             PlaybookWizardService.saveLaunch(PlaybookWizardStore.currentPlay.name, {
                //                 launch_id: launch.id,
                //                 action: 'launch',
                //             }).then(function(saved) {
                //                 // after launch
                //                 $state.go('home.playbook.dashboard.launch_job', {play_name: play.name, applicationId: saved.applicationId});
                //             });
                //         } else {
                //             // saved but not launched
                //             $state.go('home.playbook');
                //         }
                //     });
                // });
            }
        }
        console.groupEnd();
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

    launch = (play, connection, save) => {
        let launchObj = {
          "bucketsToLaunch": this.state.selectedBuckets,
          "destinationOrgId": connection.orgId,
          "destinationSysType": connection.externalSystemType,
          "destinationAccountId": connection.orgName,
          "topNCount": (this.state.limitRecommendations ? this.state.limitRecommendationsAmount : ''),
          "launchUnscored": this.state.unscored,
          "excludeItemsWithoutSalesforceId": false
        }
        actions.saveLaunch(play.name, launchObj);
        //console.log(launchObj);
    }

    render() {
        if(this.state.ratings) {
            var play = this.state.play,
                connection = this.props.connection,
                accountsCoverage = this.state.ratings,
                engineId = (accountsCoverage && accountsCoverage.ratingEngineIdCoverageMap && accountsCoverage.ratingEngineIdCoverageMap[Object.keys(accountsCoverage.ratingEngineIdCoverageMap)[0]] ? Object.keys(accountsCoverage.ratingEngineIdCoverageMap)[0] : ''),
                coverage = (engineId && accountsCoverage.ratingEngineIdCoverageMap[engineId] ? accountsCoverage.ratingEngineIdCoverageMap[engineId] : {}),
                unscoredAccountCountPercent = Math.floor((coverage.unscoredAccountCount / (coverage.unscoredAccountCount + coverage.accountCount)) * 100) || 0,
                bucketsToLaunch = play.launchHistory.mostRecentLaunch.bucketsToLaunch,
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
                <Aux>
                    <LeVPanel className={'campaign-launch'} hstretch={true}>
                        <div className={'launch-section model-ratings'}>
                            {this.makeBucketList(play, coverage, unscoredAccountCountPercent)}
                        </div>
                        <div className={'launch-section account-options'}>
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
                        <div className={'launch-section recommendations'}>
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
                                            this.nextSaveLaunch(play, connection, {saveOnly: true, lastIncompleteLaunch: play.launchHistory.lastIncompleteLaunch}); 
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
                                            this.nextSaveLaunch(play, connection, {saveOnly: false, lastIncompleteLaunch: play.launchHistory.lastIncompleteLaunch}); 
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