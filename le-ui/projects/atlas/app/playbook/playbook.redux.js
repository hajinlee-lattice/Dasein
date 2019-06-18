import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
import { store } from 'store';

var CONST = {
    FETCHING: 'FETCHING',
    FETCH_PLAY: 'FETCH_PLAY',
    FETCH_PLAYS: 'FETCH_PLAYS',
    FETCH_CONNECTIONS: 'FETCH_CONNECTIONS',
    FETCH_RATINGS: 'FETCH_RATINGS',
    SAVE_LAUNCH: 'SAVE_LAUNCH',
    SAVE_PLAY: 'SAVE_PLAY',
    ACCOUNTS_COVERAGE: 'ACCOUNTS_COVERAGE',
    EXCLUDE_ITEMS_WITHOUT_SALESFORCE_ID: 'EXCLUDE_ITEMS_WITHOUT_SALESFORCE_ID',
    DESTINATION_ACCOUNT_ID: 'DESTINATION_ACCOUNT_ID',
    ADD_PLAYBOOKWIZARDSTORE: 'ADD_PLAYBOOKWIZARDSTORE',
    FETCH_TYPES: 'FETCH_TYPES'
}

const initialState = {
    play: null,
    plays: null,
    connections: null,
    ratings: null,
    saveLaunch: null,
    accountsCoverage: null,
    excludeItemsWithoutSalesforceId: null,
    destinationAccountId: null,
    playbookWizardStore: null,
    types: null
};

export const actions = {
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
    fetchConnections: (play_name, force, deferred) => {
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
    savePlay: (opts, cb) => {
        http.post('/pls/play/', opts).then((response) => {
            store.dispatch({
                type: CONST.SAVE_PLAY,
                payload: response.data
            });
            
            if(cb && typeof cb === 'function') {
                cb();
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
        // to launch also send launchObj same way as previous method
        // launch automatically is always on, send always on true, otherwise false
        // {
        //     “playLaunch”: LaunchObj
        //     “lookupIdMap”:{ // this comes in channels api
        //       “configId”:“943d854a-ef58-4377-b002-7b3748eb6bb3",
        //       “orgId”: “Lattice_S3",
        //       “externalSystemType”: “FILE_SYSTEM”
        //     },
        //     “isAlwaysOn”:true //auto launch
        // }
        
        var opts = opts || {},
            id = opts.id || '',
            isAlwaysOn = opts.isAlwaysOn || false,
            lookupIdMap = opts.lookupIdMap || {},
            method = (id ? 'put' : 'post'),
            bucketsToLaunch = opts.bucketsToLaunch,
            cronSchedule = opts.cronSchedule,
            excludeItemsWithoutSalesforceId = opts.excludeItemsWithoutSalesforceId,
            launchUnscored = opts.launchUnscored,
            topNCount = opts.topNCount,
            launchType = opts.launchType, //FULL vs DELTA (always send FULL for now, DELTA is coming)
            launchNow = (!cronSchedule && bucketsToLaunch ? '?launch-now=true' : ''); // ?launch-now=true (if once is selected from schedule)

        var channelObj = {
            id: id,
            lookupIdMap: lookupIdMap,
            isAlwaysOn: isAlwaysOn,
            bucketsToLaunch: bucketsToLaunch,
            cronSchedule: cronSchedule,
            excludeItemsWithoutSalesforceId: excludeItemsWithoutSalesforceId,
            launchUnscored: launchUnscored,
            topNCount: topNCount,
            launchType: launchType
        };

        http[method](`/pls/play/${play_name}/channels/${id}${launchNow}`, channelObj).then((response) => {
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
        case CONST.FETCH_TYPES:
            return {
                ...state,
                types: action.payload
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