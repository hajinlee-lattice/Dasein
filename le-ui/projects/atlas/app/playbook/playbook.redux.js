import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
import { store } from 'store';

var CONST = {
    FETCH_PLAY: 'FETCH_PLAY',
    FETCH_PLAYS: 'FETCH_PLAYS',
    FETCH_CONNECTIONS: 'FETCH_CONNECTIONS',
    FETCH_RATINGS: 'FETCH_RATINGS'
}

const initialState = {
    play: null,
    plays: null,
    connections: null,
    ratings: null
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
    }
    // this.getRatingsCounts = function(ratings, noSalesForceId) {
    //     var deferred = $q.defer();
    //     $http({
    //         method: 'POST',
    //         url: this.host + '/ratingengines/coverage',
    //         data: {
    //             ratingEngineIds: ratings,
    //             restrictNotNullSalesforceId: noSalesForceId,

    //         }
    //     }).then(
    //         function onSuccess(response) {
    //             var result = response.data;
    //             deferred.resolve(result);
    //         }, function onError(response) {
    //             if (!response.data) {
    //                 response.data = {};
    //             }

    //             var errorMsg = response.data.errorMsg || 'unspecified error';
    //             deferred.resolve(errorMsg);
    //         }
    //     );
    //     return deferred.promise;
    // }
    
    // this.saveLaunch = function(play_name, opts) {
    //     var deferred = $q.defer(),
    //         opts = opts || {},
    //         launch_id = opts.launch_id || '',
    //         action = opts.action || '',
    //         launchObj = opts.launchObj || '';

    //     $http({
    //         method: 'POST',
    //         url: this.host + '/play/' + play_name + '/launches' + (launch_id ? '/' + launch_id : '') + (action ? '/' + action : ''),
    //         data: launchObj
    //     }).then(
    //         function onSuccess(response) {
    //             var result = response.data;
    //             deferred.resolve(result);
    //         }, function onError(response) {
    //             if (!response.data) {
    //                 response.data = {};
    //             }

    //             var errorMsg = response.data.errorMsg || 'unspecified error';
    //             deferred.resolve(errorMsg);
    //         }
    //     );
    //     return deferred.promise;
    // }
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
        default:
            return state;
    }
};