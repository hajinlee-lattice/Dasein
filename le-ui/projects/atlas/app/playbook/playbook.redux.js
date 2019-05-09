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
    ADD_PLAYBOOKWIZARDSTORE: 'ADD_PLAYBOOKWIZARDSTORE'
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
    playbookWizardStore: null
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
    fetchConnections: (play_name, deferred) => {
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
        httpService.get(`/pls/play/${play_name}/launches/configurations`, observer, {});
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
    savePlay: (opts) => {
        deferred = deferred || { resolve: (data) => data }

        var opts = opts || {};

        let observer = new Observer(
            response => {
                httpService.unsubscribeObservable(observer);
                store.dispatch({
                    type: CONST.SAVE_PLAY,
                    payload: response.data
                });
                return deferred.resolve(response.data);
            }
        );
        httpService.post(`/pls/play/`, opts, observer, {});
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
    saveLaunch: (play_name, opts) => {
        var opts = opts || {},
            launch_id = opts.launch_id || '',
            action = opts.action || '',
            launchObj = opts.launchObj || '',
            save = opts.save || false,
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
                    console.log(response);
                    //actions.fetchPlay(play_name);
                });
            } else {
                //actions.fetchPlay(play_name);
            } 
        });
    },
    excludeItemsWithoutSalesforceId: () => {
        store.dispatch({
            type: CONST.EXCLUDE_ITEMS_WITHOUT_SALESFORCE_ID,
            payload: response.data
        });
    },
    DestinationAccountId: () => {
        store.dispatch({
            type: CONST.DESTINATION_ACCOUNT_ID,
            payload: response.data
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