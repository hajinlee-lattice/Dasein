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

        let connection = this.props.connection,
            config = this.props.config || {},
            type = (config.name ? config.name.toLowerCase() : 'none');

        this.state = {
            play: props.play,
            ratings: null,
            coverage: null,
            // FIXME crappy hack to select all buckets because of setState recursion
            selectedBuckets: (type === 'salesforce' ? ['A','B','C','D','E','F'] : []),
            recommendationCounts: {},
            unscored: false,
            limitRecommendations: false,
            limitRecommendationsAmount: null,
            excludeItemsWithoutSalesforceId: true,
            destinationAccountId: null,
            launchAccountsCoverage: null
        };

    }

    componentDidMount() {
        let playstore = store.getState()['playbook'];

        if(playstore.play.ratingEngine) {
            if(!playstore.ratings) {
                actions.fetchRatings([playstore.play.ratingEngine.id], false);
            } else {
                this.setState({ratings: playstore.ratings});
            }
        }

        this.getLaunchAccountsCoverage(this.state.play.name, {
            sendEngineId: true,
            getExcludeItems: true,
            getDestinationAccountId: this.props.connection.lookupIdMap.accountId
        });

        this.unsubscribe = store.subscribe(this.handleChange);
    }

    componentWillUnmount() {
      this.unsubscribe();
    }

    handleChange = () => {
        const state = store.getState()['playbook'];
        this.setState(state);
    }

    getLaunchAccountsCoverage(play, opts) {
        let playstore = store.getState()['playbook'],
            vm = this;

        playstore.playbookWizardStore.launchAccountsCoverage(play.name, {
            sendEngineId: opts.sendEngineId,
            getExcludeItems: opts.getExcludeItems,
            getDestinationAccountId: opts.getDestinationAccountId
        }).then(function(response) {
            var coverage = vm.getCoverage(response).coverage,
                hasBuckets = (coverage && coverage.bucketCoverageCounts && coverage.bucketCoverageCounts.length ? true : false);

            vm.state.unscored = !hasBuckets;
            vm.state.launchAccountsCoverage = response;
            vm.setState(vm.state);
        });
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

    makeBucketList = (play, coverage, opts) => {
        /**
         * If no buckets this should produce 5 default buckets
         */
        let vm = this,
            _noBuckets = [{
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
        unscoredAccountCountPercent = opts.unscoredAccountCountPercent || 0,
        hasBuckets = (coverage && coverage.bucketCoverageCounts && coverage.bucketCoverageCounts.length),
        noBuckets = (hasBuckets ? null : _noBuckets);

        if(coverage) {
            return (
                <Aux>
                    <h2>Model Ratings</h2>
                    <LeHPanel hstretch={true} halignment={LEFT} valignment={CENTER} className={'rating-buckets'}>
                        {this.makeBuckets(coverage, play, noBuckets, opts)}
                    </LeHPanel>
                    <input id="unscored" type="checkbox" onChange={this.clickUnscored} checked={this.state.unscored} /> 
                    <label for="unscored">
                        Include the <strong>{(coverage && coverage.unscoredAccountCount ? coverage.unscoredAccountCount.toLocaleString() : 0)} ({unscoredAccountCountPercent}%)</strong> Unscored Accounts
                    </label>
                </Aux>
            );
        }
    }

    activate(play, connection, opts) {
        let vm = this,
            closeModal = (response) => {
                vm.props.closeFn();
            };

        actions.saveChannel(play.name, {
            channelId: connection.id,
            playLaunch: connection.playLaunch,
            lookupIdMap: connection.lookupIdMap,
            isAlwaysOn: !connection.isAlwaysOn,
            launchObj: opts.launchObj || {
                bucketsToLaunch: this.state.selectedBuckets,
                destinationOrgId: connection.lookupIdMap.orgId,
                destinationSysType: connection.lookupIdMap.externalSystemType,
                destinationAccountId: connection.lookupIdMap.accountId,
                topNCount: (this.state.limitRecommendations ? this.state.limitRecommendationsAmount : ''),
                launchUnscored: this.state.unscored,
                excludeItemsWithoutSalesforceId: this.state.excludeItemsWithoutSalesforceId
            }
        }, closeModal);
    }

    launch = (play, connection, opts) => {
        // save channel (saveChannel)
        // launch if launching saveLaunch() (send opts.action = launch)
        
        // FIXME crappy hack to select all buckets because of setState recursion
        var coverageObj = this.getCoverage(this.state.launchAccountsCoverage);
        this.state.selectedBuckets = this.state.selectedBuckets.splice(0,4);
        this.setState(this.state);

        var opts = opts || {},
            play = opts.play || store.getState().playbook.play,
            ratings = store.getState().playbook.ratings,
            launchObj = opts.launchObj || {
                //id: (connection.playLaunch ? connection.playLaunch.id : ''), // FIXME just a hack for now to unblock me, this shouldn't be needed
                bucketsToLaunch: this.state.selectedBuckets,
                destinationOrgId: connection.lookupIdMap.orgId,
                destinationSysType: connection.lookupIdMap.externalSystemType,
                destinationAccountId: connection.lookupIdMap.accountId,
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
            let vm = this,
                closeModal = (response) => {
                    vm.props.closeFn();
                };

            if(lastIncompleteLaunch) {
                actions.saveLaunch(play.name, {
                    engineId: opts.engineId,
                    launch_id: lastIncompleteLaunch.launchId,
                    launchObj: Object.assign({},lastIncompleteLaunch, launchObj),
                    channelId: connection.id,
                    save: true
                }, closeModal);
            } else if(lastIncompleteLaunchId) {
                actions.saveChannel(play.name, {
                    launchOpts: {
                        engineId: opts.engineId,
                        launch_id: lastIncompleteLaunchId,
                        launchObj: Object.assign({}, PlaybookWizardStore.currentPlay.launchHistory.lastIncompleteLaunch, launchObj),
                        save: save,
                        callback: closeModal
                    },
                    channelId: connection.id,
                    playLaunch: connection.playLaunch,
                    lookupIdMap: connection.lookupIdMap
                });
            } else {
                actions.saveChannel(play.name, {
                    launchOpts: {
                        engineId: opts.engineId,
                        launchObj: launchObj,
                        save: save,
                        callback: closeModal
                    },
                    playLaunch: connection.playLaunch,
                    channelId: connection.id,
                    lookupIdMap: connection.lookupIdMap,
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
        this.getLaunchAccountsCoverage(this.state.play.name, {
            sendEngineId: true,
            getExcludeItems: e.target.checked,
            getDestinationAccountId: this.props.connection.accountId
        });
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

    getCoverage(accountsCoverage) {
        var coverageType = (accountsCoverage.ratingModelsCoverageMap ? 'ratingModelsCoverageMap' : 'ratingEngineIdCoverageMap'),
            engineId,
            coverage;

        if(coverageType === 'ratingModelsCoverageMap') {
            engineId = (accountsCoverage && accountsCoverage.engineId ? accountsCoverage.engineId : '');
            coverage = (engineId && accountsCoverage[coverageType] ? accountsCoverage[coverageType] : {});
        } else {
            engineId = (accountsCoverage && accountsCoverage[coverageType] && accountsCoverage[coverageType][Object.keys(accountsCoverage[coverageType])[0]] ? Object.keys(accountsCoverage[coverageType])[0] : '');
            coverage = (engineId && accountsCoverage[coverageType][engineId] ? accountsCoverage[coverageType][engineId] : {});
        }
        return {
            engineId: engineId,
            coverage: coverage
        }
    }

    render() {
        if(this.state.launchAccountsCoverage) {
            var play = this.state.play,
                connection = this.props.connection,
                config = this.props.config,
                type = (config.name ? config.name.toLowerCase() : 'none'),
                bucketsToLaunch = (play.launchHistory.mostRecentLaunch ? play.launchHistory.mostRecentLaunch.bucketsToLaunch : []),
                coverageObj = this.getCoverage(this.state.launchAccountsCoverage),
                engineId = coverageObj.engineId,
                coverage = coverageObj.coverage,
                unscoredAccountCountPercent = Math.floor((coverage.unscoredAccountCount / (coverage.unscoredAccountCount + coverage.accountCount)) * 100) || 0,
                selectedBuckets = this.selectedBuckets,
                numAccounts = coverage.unscoredAccountCount + coverage.accountCount,
                recommendationCounts = this.makeRecommendationCounts(coverage, play),
                canLaunch = recommendationCounts.launched;

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
                    <div className={'launch-section recommendations'}>
                        <h2>{type === 'salesforce' ? 'Recommendations' : 'Contacts'} to be Launched: <strong>{recommendationCounts.launched}</strong> of {recommendationCounts.total.toLocaleString()}</h2>
                        <ul>
                            <li>
                                <input id="limitRecommendations" checked={this.state.limitRecommendations} onChange={this.clickLimitRecommendations} type="checkbox" /> 
                                <label for="limitRecommendations"> 
                                    Limit to only <input id="limitRecommendationsAmount" type="number" min="1" max={recommendationCounts.total} class={`${!this.state.limitRecommendationsAmount ? 'empty' : ''} ${this.state.limitRecommendations ? 'required' : ''}`} required={this.state.limitRecommendations} onChange={debounceEventHandler(this.enterLimitRecommendationsAmount, 200)} /> recommendations
                                </label>
                            </li>
                        </ul>
                    </div>
                    <div className={'launch-section model-ratings'}>
                        {this.makeBucketList(play, coverage, {
                            unscoredAccountCountPercent: unscoredAccountCountPercent
                        })}
                    </div>
                    <div className={'launch-section account-options'}>
                        <h2>Account Options</h2>
                        <ul>
                            <li>
                                <input id="requireAccountId" checked={true} type="checkbox" disabled={true} /> 
                                <label for="requireAccountId">Must have email</label>
                            </li>
                            <li>
                                <input id="requireAccountId" checked={this.state.excludeItemsWithoutSalesforceId} onChange={this.clickRequireAccountId} type="checkbox" /> 
                                <label for="requireAccountId">Must have account ID</label>
                            </li>
                        </ul>
                    </div>
                    <div className="launch-section schedule">
                        <h2>Launch Schedule</h2>
                        <label for="schedule">Launch</label> 
                        <select id="schedule">
                            <option>Once</option>
                            <option>Weekly</option>
                            <option>Monthly</option>
                        </select>
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
                                    disabled={!canLaunch}
                                    config={{
                                        label: "Launch Automatically",
                                        classNames: "blue-button"
                                    }}
                                    callback={() => {
                                        this.activate(play, connection, {
                                            engineId: engineId, 
                                            lastIncompleteLaunch: play.launchHistory.lastIncompleteLaunch
                                        }); 
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