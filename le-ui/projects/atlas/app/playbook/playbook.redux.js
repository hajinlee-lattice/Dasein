import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
import { store } from 'store';

var CONST = {
    FETCH_PLAY: 'FETCH_PLAY',
    FETCH_PLAYS: 'FETCH_PLAYS',
    FETCH_CONNECTIONS: 'FETCH_CONNECTIONS',
    FETCH_RATINGS: 'FETCH_RATINGS',
    SAVE_LAUNCH: 'SAVE_LAUNCH',
    SAVE_PLAY: 'SAVE_PLAY'
}

const initialState = {
    play: null,
    plays: null,
    connections: null,
    ratings: null,
    saveLaunch: null
};

export const actions = {
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
    storePlay: (opts) => {
        // do we have client session in react?
        // pass it via angular maybe
        return false;
        var deferred = $q.defer();
        var ClientSession = window.BrowserStorageUtility.getClientSession();
        opts.createdBy = opts.createdBy || ClientSession.EmailAddress;
        opts.updatedBy = ClientSession.EmailAddress;

        // how to call savePlay?
        PlaybookWizardService.savePlay(opts).then(function(data){
            PlaybookWizardStore.setPlay(data);
            deferred.resolve(data);
        });
        return deferred.promise;
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
    saveLaunch: (play_name, opts, deferred) => {
        console.log(play_name, opts);
        return false;
        var ClientSession = window.BrowserStorageUtility.getClientSession();
        console.log(ClientSession);
        console.log(play_name, opts, store.getState().playbook);
        return false;
        deferred = deferred || { resolve: (data) => data }

        var opts = opts || {},
            launch_id = opts.launch_id || '',
            action = opts.action || '',
            launchObj = opts.launchObj || '';

        let observer = new Observer(
            response => {
                httpService.unsubscribeObservable(observer);
                store.dispatch({
                    type: CONST.SAVE_LAUNCH,
                    payload: response.data
                });
                return deferred.resolve(response.data);
            }
        );
        httpService.post('/pls/play/' + play_name + '/launches' + (launch_id ? '/' + launch_id : '') + (action ? '/' + action : ''), launchObj, observer, {});
    }
};

export const reducer = (state = initialState, action) => {
    switch (action.type) {
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
        default:
            return state;
    }
};