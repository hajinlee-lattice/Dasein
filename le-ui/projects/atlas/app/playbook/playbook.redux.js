import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
import { store } from 'store';

var CONST = {
    RESET: 'RESET',
    LOADING: 'LOADING',
    FETCHING: 'FETCHING',
    FETCH_PLAY: 'FETCH_PLAY',
    FETCH_PLAYS: 'FETCH_PLAYS',
    FETCH_CONNECTIONS: 'FETCH_CONNECTIONS',
    FETCH_RATINGS: 'FETCH_RATINGS',
    FETCH_TYPES: 'FETCH_TYPES',
    FETCH_LOOKUP_ID_MAPPING: 'FETCH_LOOKUP_ID_MAPPING',
    FETCH_USER_DOCUMENT: 'FETCH_USER_DOCUMENT',
    FETCH_PROGRAMS: 'FETCH_PROGRAMS',
    FETCH_STATIC_LISTS: 'FETCH_STATIC_LISTS',
    FETCH_ACCOUNTS_DATA: 'FETCH_ACCOUNTS_DATA',
    FETCH_ACCOUNTS_COUNT: 'FETCH_ACCOUNTS_COUNT',
    SAVE_LAUNCH: 'SAVE_LAUNCH',
    SAVE_PLAY: 'SAVE_PLAY',
    ACCOUNTS_COVERAGE: 'ACCOUNTS_COVERAGE',
    EXCLUDE_ITEMS_WITHOUT_SALESFORCE_ID: 'EXCLUDE_ITEMS_WITHOUT_SALESFORCE_ID',
    DESTINATION_ACCOUNT_ID: 'DESTINATION_ACCOUNT_ID',
    ADD_PLAYBOOKWIZARDSTORE: 'ADD_PLAYBOOKWIZARDSTORE'
}

const initialState = {
    loading: false,
    play: null,
    plays: null,
    connections: null,
    ratings: null,
    saveLaunch: null,
    accountsCoverage: null,
    excludeItemsWithoutSalesforceId: null,
    destinationAccountId: null,
    playbookWizardStore: null,
    types: null,
    lookupIdMapping: null,
    userDocument: null,
    programs: null,
    staticLists: null,
    accountsCount: null,
    accountsData: null
};

export const actions = {
    reset: () => {
        store.dispatch({
            type: CONST.RESET
        });
    },
    setLoading: (state) => {
        store.dispatch({
            type: CONST.LOADING,
            payload: state
        });
    },
    addPlaybookWizardStore: (playbookWizardStore) => {
        store.dispatch({
            type: CONST.ADD_PLAYBOOKWIZARDSTORE,
            payload: playbookWizardStore
        });
    },
    fetchPlay: (play_name, deferred) => {
        deferred = deferred || { resolve: (data) => data }
        let observer = new Observer(
            response => {
                httpService.unsubscribeObservable(observer);
                store.dispatch({
                    type: CONST.FETCH_PLAY,
                    payload: response.data
                });
                return deferred.resolve(response.data);
            }
        );
        httpService.get('/pls/play/' + play_name, observer, {});
    },
    fetchPlays: (deferred) => {
        deferred = deferred || { resolve: (data) => data }
        let observer = new Observer(
            response => {
                httpService.unsubscribeObservable(observer);
                store.dispatch({
                    type: CONST.FETCH_PLAYS,
                    payload: response.data
                });
                return deferred.resolve(response.data);
            }
        );
        httpService.get('/pls/play', observer, {});
    },
    fetchConnections: (play_name, force, cb, deferred) => {
        let playstore = store.getState()['playbook'];
        if(playstore.connections && !force) {
            return false;
        }
        deferred = deferred || { resolve: (data) => data }
        let observer = new Observer(
            response => {
                httpService.unsubscribeObservable(observer);
                store.dispatch({
                    type: CONST.FETCH_CONNECTIONS,
                    payload: response.data
                });
                if(cb && typeof cb === 'function') {
                    cb();
                }
                return deferred.resolve(response.data);
            }
        );
        //httpService.get(`/pls/play/${play_name}/launches/configurations`, observer, {});
        httpService.get(`/pls/play/${play_name}/channels?include-unlaunched-channels=true`, observer, {});
    },
    fetchRatings: (ratingEngineIds, restrictNotNullSalesforceId, deferred) => {
        let playstore = store.getState()['playbook'];
        if(playstore.ratings) {
            return false;
        }
        deferred = deferred || { resolve: (data) => data }
        let observer = new Observer(
            response => {
                httpService.unsubscribeObservable(observer);
                store.dispatch({
                    type: CONST.FETCH_RATINGS,
                    payload: response.data
                });
                return deferred.resolve(response.data);
            }
        );
        httpService.post(`/pls/ratingengines/coverage`, {
            ratingEngineIds: ratingEngineIds,
            restrictNotNullSalesforceId: restrictNotNullSalesforceId
        }, observer, {});
    },
    getAccountsData: (query) => {
        deferred = deferred || { resolve: (data) => data }
        let observer = new Observer(
            response => {
                httpService.unsubscribeObservable(observer);
                store.dispatch({
                    type: CONST.FETCH_ACCOUNTS_COUNT,
                    payload: response.data
                });
                if(cb && typeof cb === 'function') {
                    cb(response.data);
                }
                return deferred.resolve(response.data);
            }
        );
        httpService.post(`/pls/accounts/data`, query, observer, {});
    },
    fetchAccountsCount: (query, cb, deferred) => {
        deferred = deferred || { resolve: (data) => data }
        let observer = new Observer(
            response => {
                httpService.unsubscribeObservable(observer);
                store.dispatch({
                    type: CONST.FETCH_ACCOUNTS_COUNT,
                    payload: response.data
                });
                if(cb && typeof cb === 'function') {
                    cb(response.data);
                }
                return deferred.resolve(response.data);
            }
        );
        // var query = { 'preexisting_segment_name': data.targetSegment.name };
        httpService.post(`/pls/accounts/count`, query, observer, {});
    },
    fetchTypes: (cb, deferred) => {
        deferred = deferred || { resolve: (data) => data }
        let observer = new Observer(
            response => {
                httpService.unsubscribeObservable(observer);
                store.dispatch({
                    type: CONST.FETCH_TYPES,
                    payload: response.data
                });
                if(cb && typeof cb === 'function') {
                    cb();
                }
                return deferred.resolve(response.data);
            }
        );
        httpService.get('/pls/playtypes', observer, {});
    },
    fetchLookupIdMapping: (cb, deferred) => {
        let playstore = store.getState()['playbook'];
        if(playstore.lookupIdMapping) {
            return false;
        }
        deferred = deferred || { resolve: (data) => data }
        let observer = new Observer(
            response => {
                httpService.unsubscribeObservable(observer);
                store.dispatch({
                    type: CONST.FETCH_LOOKUP_ID_MAPPING,
                    payload: response.data
                });
                if(cb && typeof cb === 'function') {
                    cb();
                }
                return deferred.resolve(response.data);
            }
        );
        httpService.get('/pls/lookup-id-mapping', observer, {});
    },
    fetchUserDocument: (opts, cb) => {
        var opts = opts || {};

        http.get('/tray/userdocument').then((response) => {
            /**
             * {
             *     accessToken: <useraccesstoken> for getMarketoPrograms header
             *     externalId: <external id>
             *     id: <id>
             *     name  <name>
             * }
             */
            store.dispatch({
                type: CONST.FETCH_USER_DOCUMENT,
                payload: response.data
            });
            if(cb && typeof cb === 'function') {
                cb(response.data);
            }
        });
    },
    // trayAuthenticationId is from lookup-id-mapping api when it's marketo gets externalAuthentication object which has trayAuthenticationId
    // useraccesstoken is from getTrayAuthorizationToken
    fetchPrograms: (opts, cb) => {
        // Q: what happens if nothign is returned, do we show the launch without it?
        var opts = opts || {};

        let playstore = store.getState()['playbook'];
        if(playstore && (playstore.lookupIdMapping && playstore.lookupIdMapping.MAP) && (playstore.userDocument && playstore.userDocument.accessToken)) {
            let map = playstore.lookupIdMapping.MAP.find(function(system) { return system.externalSystemName === opts.externalSystemName }),
                trayAuthenticationId = (map  && map.externalAuthentication ? map.externalAuthentication.trayAuthenticationId : null),
                useraccesstoken = playstore.userDocument.accessToken;

            http.get('/tray/marketo/programs', {
                params: {
                    trayAuthenticationId: trayAuthenticationId
                }, 
                headers: {
                    useraccesstoken: useraccesstoken
                }
            }).then((response) => {
                store.dispatch({
                    type: CONST.FETCH_PROGRAMS,
                    payload: (response.data && response.data.result ? response.data.result : [])
                });
                if(cb && typeof cb === 'function') {
                    cb(response.data);
                }
            });
        }
    },
    fetchStaticLists: (programName, opts, cb) => {
        var opts = opts || {};

        let playstore = store.getState()['playbook'];
        if(playstore && (playstore.lookupIdMapping && playstore.lookupIdMapping.MAP) && (playstore.userDocument && playstore.userDocument.accessToken)) {
            let map = playstore.lookupIdMapping.MAP.find(function(system) { return system.externalSystemName === opts.externalSystemName }),
                trayAuthenticationId = (map  && map.externalAuthentication ? map.externalAuthentication.trayAuthenticationId : null),
                useraccesstoken = playstore.userDocument.accessToken;

            http.get('/tray/marketo/staticlists', {
                params: {
                    programName: programName,
                    trayAuthenticationId: trayAuthenticationId
                }, 
                headers: {
                    useraccesstoken: useraccesstoken
                }
            }).then((response) => {
                store.dispatch({
                    type: CONST.FETCH_STATIC_LISTS,
                    payload: (response.data && response.data.result ? response.data.result : [])
                });
                if(cb && typeof cb === 'function') {
                    cb(response.data);
                }
            });
        }
    },
    savePlay: (opts, cb) => {
        actions.setLoading(true);
        http.post('/pls/play/', opts).then((response) => {
            store.dispatch({
                type: CONST.SAVE_PLAY,
                payload: response.data
            });
            actions.setLoading(false);
            if(cb && typeof cb === 'function') {
                cb(response.data);
            }
        });
    },
    savePlayLaunch: (play_name, opts) => {
        var ClientSession = window.BrowserStorageUtility.getClientSession();

        http.post(`/pls/play/`, {
            name: play_name,
            ratingEngine: {
                id: opts.engineId
            },
            createdBy: opts.createdBy || ClientSession.EmailAddress,
            updatedBy: ClientSession.EmailAddress
        }).then((response) => {
            store.dispatch({
                type: CONST.SAVE_PLAY,
                payload: response.data
            });
            actions.saveLaunch(play_name, opts);
        });
    },
    saveChannel: (play_name, opts, cb) => {
        // pls/play/{playname}/channels/{channelID} 
        // post creates new (no id)
        // put creates/updates (has id, use id as channleID)
        
        // notes: 
        // for both accountLimit and contactLimit just sending topNCount for now is fine
        // can ignore audienceType for now too
        var opts = opts || {},
            id = opts.id || '',
            isAlwaysOn = opts.isAlwaysOn || false,
            lookupIdMap = opts.lookupIdMap || {},
            method = (id ? 'put' : 'post'),
            bucketsToLaunch = opts.bucketsToLaunch,
            cronScheduleExpression = opts.cronScheduleExpression,
            excludeItemsWithoutSalesforceId = opts.excludeItemsWithoutSalesforceId,
            launchUnscored = opts.launchUnscored,
            topNCount = opts.topNCount,
            launchType = opts.launchType, //FULL vs DELTA (always send FULL for now, DELTA is coming)
            launchNow = (bucketsToLaunch ? '?launch-now=true' : ''), // this gets sent unless it's a de-activate
            channelConfig = opts.channelConfig || null;

        if(channelConfig && Object.keys(channelConfig).length === 0) {
            channelConfig = null;
        }

        var channelObj = {
            id: id,
            lookupIdMap: lookupIdMap,
            isAlwaysOn: isAlwaysOn,
            bucketsToLaunch: bucketsToLaunch,
            cronScheduleExpression: cronScheduleExpression,
            //excludeItemsWithoutSalesforceId: excludeItemsWithoutSalesforceId, // now in channelConfig
            launchUnscored: launchUnscored,
            maxAccountsToLaunch: topNCount,
            launchType: launchType,
            channelConfig: channelConfig
        };

        http[method](`/pls/play/${play_name}/channels/${id}${launchNow}`, channelObj).then((response) => {
            if(id) {
                let playstore = store.getState()['playbook'],
                    connections = playstore.connections,
                    connectionIndex = connections.findIndex(function(connection) {
                        return connection.id === id;
                    });

                connections[connectionIndex] = response.data;

                store.dispatch({
                    type: CONST.FETCH_CONNECTIONS,
                    payload: connections
                });
                if(cb && typeof cb === 'function') {
                    cb();
                }
            } else { // since no id we can't replace just the one channel, so get the whole list again
                actions.fetchConnections(play_name, true, cb);
            }
        });
    },
    saveLaunch: (play_name, opts, cb) => {
        var opts = opts || {},
            launch_id = opts.launch_id || '',
            action = opts.action || '',
            launchObj = opts.launchObj || '',
            save = opts.save || false,
            channelId = opts.channelId || '',
            getUrl = function(play_name, launch_id, action) {
                return '/pls/play/' + play_name + '/launches' + (launch_id ? '/' + launch_id : '') + (action ? '/' + action : '');
            };

        http.post(getUrl(play_name, launch_id, action), launchObj).then((response) => {
            if(save) {
                http.post(getUrl(play_name, response.data.launchId, 'launch')).then((response) => {
                    store.dispatch({
                        type: CONST.SAVE_LAUNCH,
                        payload: response.data
                    });

                    let playstore = store.getState()['playbook'],
                        play = playstore.play,
                        connections = playstore.connections,
                        connectionIndex = connections.findIndex(function(connection) {
                            return connection.id === channelId;
                        });

                    connections[connectionIndex].playLaunch = response.data;

                    store.dispatch({
                        type: CONST.FETCH_CONNECTIONS,
                        payload: connections
                    });

                    play.launchHistory.mostRecentLaunch = response.data;
                    store.dispatch({
                        type: CONST.SAVE_PLAY,
                        payload: play
                    });

                    if(cb && typeof cb === 'function') {
                        cb();
                    }
                }, function(err) {
                    if(cb && typeof cb === 'function') {
                        cb();
                    }
                });
            } 
        });
    },
    excludeItemsWithoutSalesforceId: (bool) => {
        store.dispatch({
            type: CONST.EXCLUDE_ITEMS_WITHOUT_SALESFORCE_ID,
            payload: bool
        });
    },
    destinationAccountId: (id) => {
        store.dispatch({
            type: CONST.DESTINATION_ACCOUNT_ID,
            payload: id
        });
    }
};

export const reducer = (state = initialState, action) => {
    switch (action.type) {
        case CONST.RESET:
            return initialState;
        case CONST.LOADING:
            return {
                ...state,
                loading: action.payload
            }
        case CONST.ADD_PLAYBOOKWIZARDSTORE:
            return {
                ...state,
                playbookWizardStore: action.payload
            }
        case CONST.FETCH_PLAY:
            return {
                ...state,
                play: action.payload
            }
        case CONST.FETCH_PLAYS:
            return {
                ...state,
                plays: action.payload
            }
        case CONST.FETCH_CONNECTIONS:
            return {
                ...state,
                connections: action.payload
            }
        case CONST.FETCH_RATINGS:
            return {
                ...state,
                ratings: action.payload
            }
        case CONST.FETCH_ACCOUNTS_DATA:
            return {
                ...state,
                accountsData: action.payload
            }
        case CONST.FETCH_ACCOUNTS_COUNT:
            return {
                ...state,
                accountsCount: action.payload
            }
        case CONST.FETCH_TYPES:
            return {
                ...state,
                types: action.payload
            }
        case CONST.FETCH_LOOKUP_ID_MAPPING:
            return {
                ...state,
                lookupIdMapping: action.payload
            }
        case CONST.FETCH_USER_DOCUMENT:
            return {
                ...state,
                userDocument: action.payload
            }
        case CONST.FETCH_PROGRAMS:
            return {
                ...state,
                programs: action.payload
            }
        case CONST.FETCH_STATIC_LISTS:
            return {
                ...state,
                staticLists: action.payload
            }
        case CONST.SAVE_LAUNCH:
            return {
                ...state,
                saveLaunch: action.payload
            }
        case CONST.SAVE_PLAY:
            return {
                ...state,
                play: action.payload
            }
        case CONST.ACCOUNTS_COVERAGE:
            return {
                ...state,
                accountsCoverage: action.payload
            }
        case CONST.EXCLUDE_ITEMS_WITHOUT_SALESFORCE_ID:
            return {
                ...state,
                excludeItemsWithoutSalesforceId: action.payload
            }
        case CONST.DESTINATION_ACCOUNT_ID:
            return {
                ...state,
                destinationAccountId: action.payload
            }
        default:
            return state;
    }
};