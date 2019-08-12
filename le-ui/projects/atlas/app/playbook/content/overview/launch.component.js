/**
- add description behavior is wrong

All Channels
? Include unscored accounts should be enabled/included if the campaign has no models ? blocked, /pls/count failing
? Retain last launch settings: When you eventually go back to the setting, the previous settings are no longer available. ? we don't get these settings back, exzcept the ones we do which we retain
? When you disable always on, all the previous launch data is removed, but eventually comes back after a delay ? blocked because all launches always fail
*/

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
import { deepCopy } from 'common/app/utilities/ObjectUtilities.js';
import { debounce } from 'lodash';
import {
    LEFT,
    CENTER,
    SPACEAROUND,
    SPACEBETWEEN,
    SPACEEVEN
} from "common/widgets/container/le-alignments";
import "../../../../../common/assets/sass/mixins.scss";
import "../../../../../common/assets/sass/_tooltips.scss";
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
    let list = ['Marketo','Facebook','LinkedIn'];
    if(showlist) {
        return list;
    }
    return (list.indexOf(externalSystemName) !== -1);
}

function audienceParamsDefault() { // Oh, you'll be immutable alright. I'll make you immutable
    return {
        audienceName: '',
        audienceId: '',
        folderName: ''
    }
}

class LaunchComponent extends Component {
    constructor(props) {
        super(props);

        let connection = deepCopy(this.props.connection), //deep copy
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
            launchType: null,
            keepInSync: true,
            programs: null,
            staticList: null,
            showNewAudienceName: false,
            audienceParams: audienceParamsDefault(),
            connection: connection,
            lookupIdMapping: null,
            fetching: false,
            errors: {},
            dirty: {}
        };
    }

    componentDidMount() {
        let playstore = store.getState()['playbook'],
            vm = this,
            channelConfig = this.getChannelConfig();

        this.setState({launchLoading: playstore.launchLoading});

        if(playstore.play.ratingEngine) {
            if(!playstore.ratings) {
                actions.fetchRatings([playstore.play.ratingEngine.id], false);
            } else {
                this.setState({ratings: playstore.ratings});
            }
        }

        if(!playstore.lookupIdMapping) {
            actions.fetchLookupIdMapping();
        } else {
            this.setState({lookupIdMapping: playstore.lookupIdMapping});
        }
        if(this.state.externalSystemName && isAudience(this.state.externalSystemName)) {
            actions.fetchUserDocument({}, function() {
                actions.fetchPrograms({ // get the programs list
                    externalSystemName: vm.state.externalSystemName
                }, function(data) {
                    if(data.success) {
                        let programs = (data && data.result ? data.result : []);
                        vm.state.programs = programs;
                        if(programs.length) {
                            vm.state.audienceParams.folderName = programs[0].name;
                            vm.setState(vm.state);

                            actions.fetchStaticLists(programs[0].name, {externalSystemName: vm.state.externalSystemName}, function(data) {
                                if(data && data.result) {
                                    let staticList = (data && data.result ? data.result : []);
                                    vm.state.staticList = staticList;
                                    vm.setState(vm.state);
                                }
                            });
                        } else {
                            let staticList = [];
                            vm.state.staticList = staticList;
                            vm.setState(vm.state);
                        }
                    } else if(data.message) {
                        vm.state.error = `Error retrieving ${vm.state.externalSystemName} programs. Please retry later.`; //data.message;
                        vm.setState(vm.state)
                    }
                });
            });
        }

        this.getLaunchAccountsCoverage(this.state.play.name, {
            sendEngineId: true,
            getExcludeItems: true,
            getDestinationAccountId: this.state.connection.lookupIdMap.accountId
        });

        if(this.state.externalSystemName === 'Facebook') {
            this.state.audienceParams.audienceType = 'CONTACTS';
            this.setState(this.state);
        }

        if(!channelConfig || !channelConfig.audienceName) {
            this.state.showNewAudienceName = true;
            this.setState(this.state);
        }
        if(channelConfig && channelConfig.audienceName) {
            this.state.audienceParams.audienceName = channelConfig.audienceName;
            this.setState(this.state);
        }

        this.unsubscribe = store.subscribe(this.handleChange);
    }

    componentWillUnmount() {
        actions.reset({
            FETCH_PROGRAMS: null
        });
        this.unsubscribe();
    }

    handleChange = () => {
        const state = store.getState()['playbook'];
        this.setState(state);
    }

    getChannelConfig = (externalSystemName) => {
        var externalSystemName = externalSystemName || this.state.externalSystemName;
        if(this.state.externalSystemName && this.state.connection && this.state.connection.channelConfig && this.state.connection.channelConfig[externalSystemName.toLowerCase()]) {
            return this.state.connection.channelConfig[externalSystemName.toLowerCase()];
        }
        return {};
    }

    getLaunchAccountsCoverage(play, opts) {
        let playstore = store.getState()['playbook'],
            vm = this;

        vm.state.fetching = true;
        vm.setState(this.state);

        let deepState = deepCopy(vm.state);
        
        // creates this.state.launchAccountsCoverage, needed to load launch
        if(playstore.play.ratingEngine) {
            playstore.playbookWizardStore.launchAccountsCoverage(play.name, { 
                sendEngineId: opts.sendEngineId,
                getExcludeItems: opts.getExcludeItems,
                getDestinationAccountId: opts.getDestinationAccountId
            }).then(function(response) {
                var coverage = vm.getCoverage(response).coverage,
                    hasBuckets = (coverage && coverage.bucketCoverageCounts && coverage.bucketCoverageCounts.length ? true : false);

                if(!vm.state.dirty.unscored) {
                    vm.state.unscored = !hasBuckets; // if there are buckets this should be checked by default
                }
                vm.state.launchAccountsCoverage = response;

                var coverageObj = vm.getCoverage(response),
                    coverage = coverageObj.coverage,
                    coverageBuckets = (coverage && coverage.bucketCoverageCounts && coverage.bucketCoverageCounts.length ? coverage.bucketCoverageCounts : []);

                if(coverageBuckets) { // this pre-selects all the buckets
                    vm.state.selectedBuckets = [];
                    coverageBuckets.forEach(function(bucket) {
                        vm.state.selectedBuckets.push(bucket.bucket);
                    });
                }
                vm.state.fetching = false;
                vm.setState(vm.state);
            });
        } else {
            // actions.fetchEntitiesCounts({
            //     account_restriction: playstore.play.targetSegment.account_restriction, 
            //     contact_restriction: playstore.play.targetSegment.contact_restriction
            // }, function(data) {
            //     console.log(data);
            //     // vm.state.excludeItemsWithoutSalesforceId = deepState.excludeItemsWithoutSalesforceId; // this gets reset here. I don't know exactly why.
            //     // vm.state.launchAccountsCoverage = {
            //     //     accountsCount: data
            //     // };
            //     // vm.state.fetching = false;
            //     // vm.setState(vm.state);
            // });
            actions.fetchAccountsCount({ 'preexisting_segment_name': playstore.play.targetSegment.name }, function(data) {
                vm.state.excludeItemsWithoutSalesforceId = deepState.excludeItemsWithoutSalesforceId; // this gets reset here. I don't know exactly why.
                vm.state.launchAccountsCoverage = {
                    accountsCount: data
                };
                vm.state.fetching = false;
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
                    <h2>Model Ratings</h2>
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
                        <input id="unscored" type="checkbox" onChange={this.clickUnscored} checked={this.state.unscored} disabled={this.state.fetching} /> 
                        <label for="unscored">
                            Include the {(coverage && coverage.unscoredAccountCount ? coverage.unscoredAccountCount.toLocaleString() : 0)} ({unscoredAccountCountPercent}%) Unscored Accounts
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
                        <input id="requireAccountId" checked={this.state.excludeItemsWithoutSalesforceId} onChange={this.clickRequireAccountId} type="checkbox" /> 
                        <label for="requireAccountId">Must have account ID</label>
                    </li>
                </Aux>
            );
        } else if(['Marketo','Facebook'].indexOf(externalSystemName) !== -1) {
            return (
                <Aux>
                    <li>
                        <input id="requireContactIfo" checked={true} disabled={true} type="checkbox" /> 
                        <label for="requireContactIfo">Must have contact info</label>
                    </li>
                </Aux>
            );
        } else if(externalSystemName === 'LinkedIn') {
            return (
                <Aux>
                    <li>
                        <input id="requireAccountName" checked={true} disabled={true} type="checkbox" /> 
                        <label for="requireAccountName">Must have website or account name</label>
                    </li>
                </Aux>
            );
        }
    }

    makeProgramsList(programs) {
        // program name = folder name
        // static list name dropdwon is audience name/id
        // static list name input is audience name/no id (makes new audience)
        var vm = this,
            list = [],
            channelConfig = this.getChannelConfig();

        var newAudienceNameInput = [];
        if(this.state.showNewAudienceName) {
            newAudienceNameInput.push(
                <Aux>
                    <input id={'newAudienceName'} name={'newAudienceName'} className={`${this.state.errors.audiencename ? 'error' : ''}`} onBlur={(event) => {
                        var oldAudience = null;
                        if(vm.state.staticList && vm.state.staticList.length && event.target.value) {
                            oldAudience = vm.state.staticList.find(function(item) {
                                return item.name === event.target.value;
                            });
                            if(oldAudience) {
                                this.state.errors.audiencename = `* A static list with this name already exists. Please choose another.`;
                                vm.setState({errors: this.state.errors});
                                return false; // stop here
                            }
                        }
                        this.state.errors.audiencename = null;
                        this.state.audienceParams.audienceName = event.target.value;
                        this.setState(this.state);
                    }} />
                </Aux>
            );
        }
        if(programs && programs.length) {
            programs.forEach(function(program) {
                let selected = (channelConfig.folderName === program.name);
                list.push(<option seleted={selected}>{program.name}</option>);
            });
            return (
                <div className={'launch-section programs'}>
                    <LeVPanel halignment={LEFT} valignment={CENTER} className={'program-settings'}>
                        <LeHPanel hstretch={true} halignment={LEFT} valignment={CENTER} className={'programName-container'}>
                            <label for={'programName'}>{this.state.externalSystemName} Program Name</label>
                            <select id={'programName'} onChange={(event) => {
                                let programName = event.target.value;

                                if(channelConfig.folderName === programName) {
                                    this.state.audienceParams.audienceName = this.state.audienceParams.audienceName || channelConfig.audienceName;
                                }
                                // if(channelConfig.audienceName === this.state.audienceParams.audienceName) {
                                //     this.state.showNewAudienceName = false; // this isn't working because of a timing issue
                                // }
                                this.state.errors.audiencename = null;
                                this.getStaticList(programName);
                                this.setState(this.state);
                            }}>
                                {list}
                            </select>
                        </LeHPanel>
                        <LeHPanel hstretch={true} halignment={LEFT} valignment={CENTER} className={'staticList-container'}>
                            <label for={'staticList'}>{this.state.externalSystemName} Audience</label>
                            {vm.makeStaticList(this.state.staticList)}
                            {newAudienceNameInput}
                        </LeHPanel>
                        <LeHPanel hstretch={true} halignment={LEFT} valignment={CENTER} className={'audience-error-container red-text'}>
                            {this.state.errors.audiencename}
                        </LeHPanel>
                    </LeVPanel>
                </div>
            );
        } else if(this.state.staticList) {
            return (
                <div className={'launch-section programs'}>
                    <LeVPanel halignment={LEFT} valignment={CENTER} className={'program-settings'}>
                        <LeHPanel hstretch={true} halignment={LEFT} valignment={CENTER} className={'staticList-container'}>
                            <label for={'staticList'}>{this.state.externalSystemName} Audience</label>
                            {vm.makeStaticList(this.state.staticList)}
                            {newAudienceNameInput}
                        </LeHPanel>
                        <LeHPanel hstretch={true} halignment={LEFT} valignment={CENTER} className={'audience-error-container red-text'}>
                            {this.state.errors.audiencename}
                        </LeHPanel>
                    </LeVPanel>
                </div>
            );
        }
    }

    makeStaticList(list) {
        var vm = this,
            options = [],
            channelConfig = this.getChannelConfig(),
            hasSelected = false;

        if(list) {
            if(!list.length || !list[0].loadingState) {
                options.push(<option value={''}>-- Create new list --</option>);
            }
            list.forEach(function(item) {
                let selected = (channelConfig.audienceName === item.name);
                hasSelected = hasSelected || selected;
                
                options.push(<option value={item.id} selected={selected}>{item.name}</option>);
            });

            return(
                <select id={'staticList'} className={`${(hasSelected ? 'hasSelected' : '')}`} onChange={(event) => {
                    if(!this.state.showNewListInput && event.target.value === '') {
                        this.state.showNewAudienceName = true;
                    } else {
                        let item = list.find(function(_item) { return (_item.id == event.target.value) });
                        this.state.showNewAudienceName = false;
                        this.state.audienceParams.audienceName = (item && item.name ? item.name : '');
                    }
                    this.state.audienceParams.audienceId = event.target.value;
                    this.state.errors.audiencename = null;
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
        vm.state.showNewAudienceName = false;

        vm.state.audienceParams = audienceParamsDefault(); // reset this
        vm.state.audienceParams.folderName = programName;

        vm.setState(vm.state);

        actions.fetchStaticLists(programName, {externalSystemName: vm.state.externalSystemName}, function(data) {
            if(data && data.result) {
                let staticList = (data && data.result ? data.result : []);
                vm.state.showNewAudienceName = true;
                vm.state.staticList = staticList;
                vm.setState(vm.state);
            }
        });
    }

    makeDropFolder(externalSystemName, lookupIdMapping) {
        var system = null;
        if(externalSystemName && lookupIdMapping && lookupIdMapping.FILE_SYSTEM) {
            system = lookupIdMapping.FILE_SYSTEM.find(function(item) {
                return (item.externalSystemName === externalSystemName);
            });
            if(system && externalSystemName === 'AWS_S3') {
                return(
                    <div className={'launch-section drop-folder'}>
                        <h2>
                            S3 Drop Folder 
                            <i className={'more-info show-tooltip left bottom'}> i
                                <div className={'tooltip_'}>
                                    <div className={'cover'}>
                                        <p>Data Rentention</p>
                                        <p>Audience data generated on S3 will be automatically removed after 30 days.</p>
                                        <p>Identifying Audience Data</p>
                                        <p>Audience data (accounts and contacts with-in accounts) will be available as CSV formatted file</p>
                                    </div>
                                </div>
                            </i>
                        </h2>
                        <p className={'folder'}>
                            <strong>{system.exportFolder}</strong>
                        </p>
                        <p className={'subtext'}>
                            After launching the campaign, audience data is generated and will be available in the S3 drop location. 
                            You will need the access token to S3 to access this data. 
                            You can obtain token from the connection page.
                        </p>
                    </div>
                );
            }
        }
    }

    makeAudienceType(externalSystemName) {
        var vm = this;
        let inputs = [];
        inputs.push({
            name: 'accounts',
            displayName: 'Accounts',
            type: 'radio',
            selected: null,
            disabled: (externalSystemName === 'Facebook' ? true : false)
        });
        inputs.push({
            name: 'contacts',
            displayName: 'Contacts',
            type: 'radio',
            selected: (externalSystemName === 'Facebook' ? true : null),
            disabled: false
        });
        var items = [];
        inputs.forEach(function(item) {
            items.push(
                <div>
                    <input name="audiencetype" value={item.name.toUpperCase()} id={`${item.type}-${item.name}`} type={item.type} disabled={item.disabled} checked={item.selected} onChange={vm.clickAudienceType} />
                    <label for={`${item.type}-${item.name}`}>{item.displayName}</label>
                </div>
            )
        });
        if(['LinkedIn','Facebook'].indexOf(externalSystemName) !== -1) {
            return(
                <div className={'launch-section audience-type'}>
                    <LeHPanel hstretch={true} halignment={LEFT} valignment={CENTER} className={'audienceType-container'}>
                        {items}
                    </LeHPanel>
                    <p>
                        After this audience is sent to {externalSystemName}, it will take 24 - 48 hours to be available for use.
                    </p>
                </div>
            );
        }
    }

    makeKeepInSync() {
        if(this.state.launchSchedule) {
            return (
                <div className={'keep-in-sync'}>
                    <input id={'keepInSync'} type={'checkbox'} onChange={this.clickKeepInSync} checked={this.state.keepInSync} /> <label for={'keepInSync'}>Keep target system in sync</label>
                    <i className={'more-info show-tooltip left top'}> i
                        <div className={'tooltip_'}>
                            <div className={'cover'}>
                                <p>We can keep adding to the target system or we can keep it in sync.</p>
                            </div>
                        </div>
                    </i>
                </div>
            )
        }
    }

    launch = (play, connection, opts) => {
        var debug = false;
        // FIXME crappy hack to select all buckets because of setState recursion
        var coverageObj = this.getCoverage(this.state.launchAccountsCoverage);
        this.state.selectedBuckets = this.state.selectedBuckets.splice(0,4);
        if(!debug) {
            this.state.launchingState = 'launching';
        }
        this.setState(this.state);

        var opts = opts || {},
            debug = opts.debug || debug,
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
            supressAccountsWithoutLookupId: this.state.excludeItemsWithoutSalesforceId,
            audienceId: '',
            audienceName: '',
            folderName: ''
        };

        var channelConfig = connection.channelConfig || channelConfigDefault;

        if(this.state.audienceParams) {
            channelConfig[channelConfigKey].audienceId = this.state.audienceParams.audienceId;
            channelConfig[channelConfigKey].audienceName = this.state.audienceParams.audienceName;

            if(this.state.audienceParams.folderName) {
                channelConfig[channelConfigKey].folderName = this.state.audienceParams.folderName;
            }
            if(this.state.audienceParams.audienceType) {
                channelConfig[channelConfigKey].audienceType = this.state.audienceParams.audienceType;
            }
        }

        if(play) {
            let vm = this,
                closeModal = (response) => {
                    this.state.launchingState = 'unlaunching';
                    this.setState(this.state);
                    vm.props.closeFn();
                };

            if(lastIncompleteLaunch) {
                launchObj = Object.assign({}, lastIncompleteLaunch, launchObj);
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
                launchType: this.state.launchType || 'FULL', // keep in sync = DIFFERENTIAL, not checked = 'FULL'
                channelConfig: channelConfig,
                debug: debug
            }, closeModal);
        }
    }

    clickUnscored = (e) => {
        this.state.dirty.unscored = true;
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
        var vm = this;
        this.state.excludeItemsWithoutSalesforceId = e.target.checked;
        this.setState(this.state);
        this.getLaunchAccountsCoverage(this.state.play.name, {
            sendEngineId: true,
            getExcludeItems: e.target.checked,
            getDestinationAccountId: this.state.connection.lookupIdMap.accountId
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
        var schedule = (e.target.value === 'Once' ? null : e.target.value),
            launchType;

        this.state.launchSchedule = chron(schedule);

        if(e.target.value === 'Once') {
            launchType = 'FULL';
        } else {
            if(this.state.keepInSync) {
                launchType = 'DIFFERENTIAL';
            } else {
                launchType = 'FULL';
            }
        }

        this.state.launchType = launchType;
        if(!schedule) {
            this.state.launchType = 'FULL';
        }
        this.setState(this.state);
    }

    clickAudienceType = (e) => {
        this.state.audienceParams.audienceType = e.target.value;
        this.setState(this.state);
    }

    clickKeepInSync = (e) => {
        // keep in sync = DIFFERENTIAL, not checked = 'FULL'
        if(e.target.checked) {
            this.state.launchType = 'DIFFERENTIAL';
        }
        this.state.keepInSync = e.target.checked;
        this.setState(this.state);
    }

    getCoverageType(accountsCoverage) {
        if(accountsCoverage && accountsCoverage.ratingModelsCoverageMap) {
            return 'ratingModelsCoverageMap';
        }
        if(accountsCoverage && accountsCoverage.ratingEngineIdCoverageMap) {
            return 'ratingEngineIdCoverageMap';
        }
        if(accountsCoverage && accountsCoverage.accountsCount) {
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
        if(this.state.externalSystemName === 'AWS_S3') {
            loaded = (this.state.lookupIdMapping);
        }
        if(loaded) {
            var play = this.state.play,
                connection = this.state.connection,
                config = this.props.config,
                type = (config.name ? config.name.toLowerCase() : 'none'),
                bucketsToLaunch = (play.launchHistory.mostRecentLaunch ? play.launchHistory.mostRecentLaunch.bucketsToLaunch : []),
                coverageObj = this.getCoverage(this.state.launchAccountsCoverage),
                engineId = coverageObj.engineId,
                coverage = coverageObj.coverage || {},
                unscoredAccountCountPercent = Math.floor((coverage.unscoredAccountCount / (coverage.unscoredAccountCount + coverage.accountCount)) * 100) || 0,
                selectedBuckets = this.selectedBuckets,
                numAccounts = coverage.unscoredAccountCount + coverage.accountCount,
                recommendationCounts = this.makeRecommendationCounts(coverage, play),
                canLaunch = recommendationCounts.launched,
                lookupIdMapping = this.state.lookupIdMapping,
                externalSystemName = this.state.externalSystemName,
                lastLaunch = (connection.lastLaunch ? connection.lastLaunch : null),
                toBeLaunchedType = (externalSystemName) => {
                    let types = {
                        default: 'Accounts',
                        Facebook: 'Contacts'
                    };
                    if(externalSystemName && types[externalSystemName]) {
                        return types[externalSystemName];
                    }
                    return types['default'];
                };

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
                        {this.makeAudienceType(externalSystemName)}
                        <div className={'launch-section recommendations'}>
                            <h2>{toBeLaunchedType(externalSystemName)} to be Launched: <strong>{recommendationCounts.launched}</strong> of {recommendationCounts.total.toLocaleString()}</h2>
                            <ul>
                                <li>
                                    <input id="limitRecommendations" checked={this.state.limitRecommendations} onChange={this.clickLimitRecommendations} type="checkbox" /> 
                                    <label for="limitRecommendations"> 
                                        Limit to only 
                                        <input id="limitRecommendationsAmount" type="number" min="1" max={recommendationCounts.total} class={`${!this.state.limitRecommendationsAmount ? 'empty' : ''} ${this.state.limitRecommendations ? 'required' : ''}`} required={this.state.limitRecommendations} onChange={debounceEventHandler(this.enterLimitRecommendationsAmount, 200)} /> 
                                        {toBeLaunchedType(externalSystemName).toLowerCase()}
                                    </label>
                                </li>
                            </ul>
                        </div>
                        {this.makeDropFolder(externalSystemName, lookupIdMapping)}
                        <div className={'launch-section model-ratings'}>
                            {this.makeBucketList(play, coverage, {
                                unscoredAccountCountPercent: unscoredAccountCountPercent
                            })}
                            <ul>
                                {this.makeAccountOptions()}
                            </ul>
                        </div>
                        <div className={'launch-section account-options'}>
                        </div>
                        {this.makeProgramsList(this.state.programs)}
                        <div className="launch-section schedule">
                            <LeHPanel hstretch={true} halignment={LEFT} valignment={CENTER} className={'schedule-dropdown-container'}>
                                <label for="schedule">Launch</label>
                                <div className={'schedule-dropdown'}>
                                    <select id="schedule" onChange={this.clickLaunchSchedule}>
                                        <option>Once</option>
                                        <option>Weekly</option>
                                        <option>Monthly</option>
                                    </select>
                                    {this.makeKeepInSync()}
                                </div>
                            </LeHPanel>
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
                                            classNames: "blue-button"
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
        } else if(this.state.error) {
            return(
                <Aux>
                    {this.state.error}
                </Aux>
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