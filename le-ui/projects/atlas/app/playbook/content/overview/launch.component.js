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

function chron(frequency) {
    var date = new Date(),
        daysofweek = 'SUN,MON,TUE,WED,THU,FRI,SAT'.split(','),
        dayofweek = daysofweek[date.getDay()],
        day = date.getDate(),
        months = 1;

    var schedule = {
        Once: null,
        Weekly: `0 0 12 ? * ${dayofweek} *`,
        Monthly: `0 0 12 ${day} 1/${months} ? *`
    }

    return schedule[frequency];
}

function isAudience(externalSystemName, showlist) {
    let list = ['Marketo'];
    if(showlist) {
        return list;
    }
    return (list.indexOf(externalSystemName) !== -1);
}

function audienceParamsDefault() { // Oh, you'll be immutable alright.  I'll make you immutable
    return {
        audienceName: '',
        audienceId: '',
        folderName: ''
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
            externalSystemName: (connection && connection.lookupIdMap && connection.lookupIdMap.externalSystemName ? connection.lookupIdMap.externalSystemName : null),
            ratings: null,
            coverage: null,
            // FIXME crappy hack to select all buckets because of setState recursion
            selectedBuckets: [], // removing, getLaunchAccountsCoverage now pre-selects // old (type === 'salesforce' ? ['A','B','C','D','E','F'] : []),
            recommendationCounts: {},
            unscored: false,
            limitRecommendations: false,
            limitRecommendationsAmount: null,
            excludeItemsWithoutSalesforceId: true,
            destinationAccountId: null,
            launchAccountsCoverage: null,
            launchSchedule: null,
            launchingState: 'unlaunching',
            programs: null,
            staticList: null,
            showNewFolderName: true,
            audienceParams: audienceParamsDefault()
        };
    }

    componentDidMount() {
        let playstore = store.getState()['playbook'],
            vm = this;
        
        this.setState({launchLoading: playstore.launchLoading});

        if(playstore.play.ratingEngine) {
            if(!playstore.ratings) {
                actions.fetchRatings([playstore.play.ratingEngine.id], false);
            } else {
                this.setState({ratings: playstore.ratings});
            }
        }

        if(this.state.externalSystemName && isAudience(this.state.externalSystemName)) {
            actions.fetchUserDocument({}, function() {
                actions.fetchPrograms({ // get the programs list
                    externalSystemName: vm.state.externalSystemName
                }, function(data) {
                    let programs = (data && data.result ? data.result : []);
                    vm.state.programs = programs;
                    vm.state.audienceParams.audienceName = programs[0].name;
                    vm.setState(vm.state);

                    actions.fetchStaticLists(programs[0].name, {externalSystemName: vm.state.externalSystemName}, function(data) {
                        if(data && data.result) {
                            let staticList = (data && data.result ? data.result : []);
                            vm.state.staticList = staticList;
                            vm.setState(vm.state);
                        }
                    });
                });
            });
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

        // creates this.state.launchAccountsCoverage, needed to load launch
        if(playstore.play.ratingEngine) {
            playstore.playbookWizardStore.launchAccountsCoverage(play.name, { 
                sendEngineId: opts.sendEngineId,
                getExcludeItems: opts.getExcludeItems,
                getDestinationAccountId: opts.getDestinationAccountId
            }).then(function(response) {
                var coverage = vm.getCoverage(response).coverage,
                    hasBuckets = (coverage && coverage.bucketCoverageCounts && coverage.bucketCoverageCounts.length ? true : false);

                vm.state.unscored = !hasBuckets;
                vm.state.launchAccountsCoverage = response;
                console.log(response);

                var coverageObj = vm.getCoverage(response),
                    coverage = coverageObj.coverage,
                    coverageBuckets = (coverage && coverage.bucketCoverageCounts && coverage.bucketCoverageCounts.length ? coverage.bucketCoverageCounts : []);

                if(coverageBuckets) { // this pre-selects all the buckets
                    coverageBuckets.forEach(function(bucket) {
                        vm.state.selectedBuckets.push(bucket.bucket);
                    });
                }

                vm.setState(vm.state);
            });
        } else {
            actions.fetchAccountsCount({ 'preexisting_segment_name': playstore.play.targetSegment.name }, function(data) {
                console.log('fetchAccountsCount', data);
                vm.state.launchAccountsCoverage = {
                    accountsCount: data
                };
                vm.setState(vm.state);
            });
        }
    }

    makeRecommendationCounts(coverage, play) {
        var vm = this; // placeholder

        var sections = {
                total: 0,
                selected: 0,
                suppressed: 0,
                launched: 0,
                contacts: 0
            },
            buckets = {};

        if(!coverage || !coverage.bucketCoverageCounts) {
            this.state.recommendationCounts = null;
            return sections;
        }
        sections.total = play.targetSegment.accounts || coverage.unscoredAccountCount;

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

    makeBuckets(coverage, play, buckets, opts) {
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
                    <div className={`bucket ${(selected ? 'selected' : '')}`} disabled={(bucket.count <= 0)} onClick={() => { if(bucket.count) { vm.bucketClick(bucket, coverage, play) } }}>
                        <div className={'bucket-text'} title={`${bucket.count} accounts (${percent}%)`}>
                            <h3>{bucket.bucket}</h3>
                            <em>{bucket.count} ({percent}%)</em>
                        </div>
                    </div>
                );
            });
        }
        return _buckets;
    }

    makeBucketsContainer(coverage, play, noBuckets, opts) {
        var _buckets = [],
            total = 0,
            vm = this,
            coverageBuckets = (coverage && coverage.bucketCoverageCounts && coverage.bucketCoverageCounts.length ? coverage.bucketCoverageCounts : []),
            buckets = buckets || coverageBuckets,
            littleBuckets = true;

        if(buckets && buckets.length) {
            return(
                <Aux>
                    <h2>Ratings</h2>
                    <LeHPanel 
                        hstretch={!littleBuckets} 
                        halignment={LEFT} 
                        valignment={CENTER} 
                        className={`rating-buckets ${littleBuckets ? 'little-buckets' : ''}`}
                    >
                        {this.makeBuckets(coverage, play, noBuckets, opts)}
                    </LeHPanel>
                </Aux>
            );
        }
    }

    makeBucketList = (play, coverage, opts) => {
        let unscoredAccountCountPercent = opts.unscoredAccountCountPercent || 0;
        /**
         * If no buckets this should produce 5 default buckets
         */
        var vm = this,
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
            hasBuckets = (coverage && coverage.bucketCoverageCounts && coverage.bucketCoverageCounts.length),
            noBuckets = (hasBuckets ? null : null); //_noBuckets);

        if(coverage) {
            return (
                <Aux>
                    {this.makeBucketsContainer(coverage, play, noBuckets, opts)}
                    <div className={'unscored-accounts-container'}>
                        <input id="unscored" type="checkbox" onChange={this.clickUnscored} checked={this.state.unscored} /> 
                        <label for="unscored">
                            Include the <strong>{(coverage && coverage.unscoredAccountCount ? coverage.unscoredAccountCount.toLocaleString() : 0)} ({unscoredAccountCountPercent}%)</strong> unscored accounts
                        </label>
                    </div>
                </Aux>
            );
        }
    }

    makeAccountOptions() {
        let externalSystemName = this.state.externalSystemName;

        if(externalSystemName === 'Salesforce') {
            return (
                <Aux>
                    <li>
                        <input id="requireEmail" checked={true}  disabled={true} type="checkbox" /> 
                        <label for="requireEmail">Must have email</label>
                    </li>
                    <li>
                        <input id="requireAccountId" checked={this.state.excludeItemsWithoutSalesforceId} onChange={this.clickRequireAccountId} type="checkbox" /> 
                        <label for="requireAccountId">Must have account ID</label>
                    </li>
                </Aux>
            );
        } else if(externalSystemName === 'Marketo') {
            return (
                <Aux>
                    <li>
                        <input id="requireContactIfo" checked={true} disabled={true} type="checkbox" /> 
                        <label for="requireContactIfo">Must have contact info</label>
                    </li>
                </Aux>
            );
        }
    }


    makeProgramsList(programs) {
        var vm = this,
            list = [];

        var newFolderNameInput = [];
        if(this.state.showNewFolderName) {
            newFolderNameInput.push(<input name={'newFolderName'} onBlur={(event) => {
                this.state.audienceParams.folderName = event.target.value;
                this.setState(this.state);
            }} />);
        }

        if(programs && programs.length) {
            programs.forEach(function(program) {
                list.push(<option>{program.name}</option>);
            });
            return (
                <div className={'launch-section programs'}>
                    <h2>{this.state.externalSystemName} Destination List</h2>
                    <LeVPanel halignment={LEFT} valignment={CENTER} className={'program-settings'}>
                        <LeHPanel hstretch={true} halignment={LEFT} valignment={CENTER} className={'programName-container'}>
                            <label for={'programName'}>Program name</label>
                            <select id={'programName'} onChange={(event) => { 
                                this.getStaticList(event.target.value);
                            }}>
                                {list}
                            </select>
                        </LeHPanel>
                        <LeHPanel hstretch={true} halignment={LEFT} valignment={CENTER} className={'staticList-container'}>
                            <label for={'staticList'}>Static list name</label>
                            {vm.makeStaticList(this.state.staticList)}
                            {newFolderNameInput}
                        </LeHPanel>
                    </LeVPanel>

                </div>
            );
        }
    }

    makeStaticList(list) {
        var options = [];

        if(list) {
            if(!list[0].loadingState) {
                options.push(<option value={''}>-- Create new list --</option>);
            }
            list.forEach(function(item) {
                options.push(<option value={item.id}>{item.name}</option>);
            });

            return(
                <select id={'staticList'} onChange={(event) => {
                    if(!this.state.showNewListInput && event.target.value === '') {
                        this.state.showNewFolderName = true;
                    } else {
                        let item = list.find(function(_item) { return (_item.id == event.target.value) });
                        this.state.showNewFolderName = false;
                        this.state.audienceParams.folderName = (item && item.name ? item.name : '');
                    }
                    this.state.audienceParams.audienceId = event.target.value;
                    this.setState(this.state);
                }}>
                    {options}
                </select>
            );
        }
    }

    getStaticList(programName) {
        var vm = this;

        vm.state.staticList = [{name: 'loading...', loadingState: true}];
        vm.state.showNewFolderName = false;

        vm.state.audienceParams = audienceParamsDefault(); // reset this
        vm.state.audienceParams.audienceName = programName;

        vm.setState(vm.state);

        actions.fetchStaticLists(programName, {externalSystemName: vm.state.externalSystemName}, function(data) {
            if(data && data.result) {
                let staticList = (data && data.result ? data.result : []);
                vm.state.showNewFolderName = true;
                vm.state.staticList = staticList;
                vm.setState(vm.state);
            }
        });
    }

    launch = (play, connection, opts) => {
        // FIXME crappy hack to select all buckets because of setState recursion
        var coverageObj = this.getCoverage(this.state.launchAccountsCoverage);
        this.state.selectedBuckets = this.state.selectedBuckets.splice(0,4);
        this.state.launchingState = 'launching';
        this.setState(this.state);

        var opts = opts || {},
            play = opts.play || store.getState().playbook.play,
            ratings = store.getState().playbook.ratings,
            launchObj = opts.launchObj || {
                bucketsToLaunch: this.state.selectedBuckets,
                destinationOrgId: connection.lookupIdMap.orgId,
                destinationSysType: connection.lookupIdMap.externalSystemType,
                destinationAccountId: connection.lookupIdMap.accountId,
                topNCount: (this.state.limitRecommendations ? this.state.limitRecommendationsAmount : ''),
                launchUnscored: this.state.unscored,
                excludeItemsWithoutSalesforceId: this.state.excludeItemsWithoutSalesforceId,
                launchSchedule: this.state.launchSchedule
                // audienceName: PlaybookWizardStore.getAudienceName(), //?
                // audienceId: PlaybookWizardStore.getAudienceId(), //?
                // folderName: PlaybookWizardStore.getMarketoProgramName(), //?
            },
            save = opts.save || false,
            lastIncompleteLaunchId = (play.launchHistory.lastIncompleteLaunch ? play.launchHistory.lastIncompleteLaunch.launchId : ''),
            lastIncompleteLaunch = opts.lastIncompleteLaunch || null,
            channelConfigKey = (this.state.externalSystemName ? this.state.externalSystemName.toLowerCase() : null);


        var channelConfigDefault = {};
        channelConfigDefault[channelConfigKey] = {
            supressAccountWithoutAccountId: this.state.excludeItemsWithoutSalesforceId,
            audienceId: '',
            audienceName: '',
            folderName: ''
        };

        var channelConfig = connection.channelConfig || channelConfigDefault;


        if(this.state.audienceParams && this.state.audienceParams.audienceName && this.state.audienceParams.folderName) {
            channelConfig[channelConfigKey].audienceId = this.state.audienceParams.audienceId;
            channelConfig[channelConfigKey].audienceName = this.state.audienceParams.audienceName;
            channelConfig[channelConfigKey].folderName = this.state.audienceParams.folderName;
        }

        if(play) {
            let vm = this,
                closeModal = (response) => {
                    this.state.launchingState = 'unlaunching';
                    this.setState(this.state);
                    vm.props.closeFn();
                };

            if(lastIncompleteLaunch) {
                launchObj = Object.assign({},lastIncompleteLaunch, launchObj);
            } else if(lastIncompleteLaunchId) {
                launchObj = Object.assign({}, PlaybookWizardStore.currentPlay.launchHistory.lastIncompleteLaunch, launchObj);
            } else {
                launchObj = launchObj;
            }

            actions.saveChannel(play.name, {
                id: connection.id,
                lookupIdMap: connection.lookupIdMap,
                isAlwaysOn: (!launchObj.launchSchedule ? false : !connection.isAlwaysOn),
                bucketsToLaunch: launchObj.bucketsToLaunch,
                cronScheduleExpression: launchObj.launchSchedule, //cronSchedule, //?
                excludeItemsWithoutSalesforceId: launchObj.excludeItemsWithoutSalesforceId,
                launchUnscored: launchObj.launchUnscored,
                topNCount: launchObj.topNCount,
                launchType: 'FULL',
                channelConfig: channelConfig
            }, closeModal);
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

    clickLaunchSchedule= (e) => {
        var schedule = (e.target.value === 'Once' ? null : e.target.value);
        this.state.launchSchedule = chron(schedule);
        this.setState(this.state);
    }

    getCoverageType(accountsCoverage) {
        if(accountsCoverage.ratingModelsCoverageMap) {
            return 'ratingModelsCoverageMap';
        }
        if(accountsCoverage.ratingEngineIdCoverageMap) {
            return 'ratingEngineIdCoverageMap';
        }
        if(accountsCoverage.accountsCount) {
            return 'accountsCount';
        }
        
    }

    getCoverage(accountsCoverage) {
        var coverageType = this.getCoverageType(accountsCoverage),
            engineId,
            coverage;

        if(coverageType === 'ratingModelsCoverageMap') {
            engineId = (accountsCoverage && accountsCoverage.engineId ? accountsCoverage.engineId : '');
            coverage = (engineId && accountsCoverage[coverageType] ? accountsCoverage[coverageType] : {});
        } else if(coverageType === 'ratingEngineIdCoverageMap') {
            engineId = (accountsCoverage && accountsCoverage[coverageType] && accountsCoverage[coverageType][Object.keys(accountsCoverage[coverageType])[0]] ? Object.keys(accountsCoverage[coverageType])[0] : '');
            coverage = (engineId && accountsCoverage[coverageType][engineId] ? accountsCoverage[coverageType][engineId] : {});
        } else if(coverageType === 'accountsCount') {
            engineId: null;
            coverage = { 
                unscoredAccountCount: accountsCoverage.accountsCount,
                bucketCoverageCounts: []
            };
        }

        return {
            engineId: engineId,
            coverage: coverage
        }
    }

    render() {
        var loaded = (this.state.launchAccountsCoverage);
        if(isAudience(this.state.externalSystemName)) {
            /**
             * I set this.state.programs to an empty array, from null, if the API doesn't send anything back 
             * so the modal will wait for the response in this case, but it will still load even if it's empty
             */
            loaded = loaded && (this.state.programs && this.state.staticList);
        }
        if(loaded) {
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
                <LeVPanel className={`campaign-launch ${this.state.launchingState}`} hstretch={true}>
                    <div className="campaign-launch-container">
                        <div className={'launch-section recommendations'}>
                            <h2>Accounts to be Launched: <strong>{recommendationCounts.launched}</strong> of {recommendationCounts.total.toLocaleString()}</h2>
                            <ul>
                                <li>
                                    <input id="limitRecommendations" checked={this.state.limitRecommendations} onChange={this.clickLimitRecommendations} type="checkbox" /> 
                                    <label for="limitRecommendations"> 
                                        Limit to only 
                                        <input id="limitRecommendationsAmount" type="number" min="1" max={recommendationCounts.total} class={`${!this.state.limitRecommendationsAmount ? 'empty' : ''} ${this.state.limitRecommendations ? 'required' : ''}`} required={this.state.limitRecommendations} onChange={debounceEventHandler(this.enterLimitRecommendationsAmount, 200)} /> 
                                        accounts
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
                            <ul>
                                {this.makeAccountOptions()}
                            </ul>
                        </div>
                        {this.makeProgramsList(this.state.programs)}
                        <div className="launch-section schedule">
                            <label for="schedule">Launch</label> 
                            <select id="schedule" onChange={this.clickLaunchSchedule}>
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
                                        name="launchnow"
                                        disabled={!canLaunch}
                                        config={{
                                            label: "Launch Now",
                                            classNames: "white-button"
                                        }}
                                        callback={() => { 
                                            this.launch(play, connection, {
                                                engineId: engineId, 
                                                lastIncompleteLaunch: play.launchHistory.lastIncompleteLaunch
                                            }); 
                                        }} />
                                </li>
                            </ul>
                        </div>
                    </div>
                </LeVPanel>
            );
        } else {
            return (
                <Aux>
                    <div className="loader"></div>
                </Aux>
            );
        }
    }
}
export default LaunchComponent;