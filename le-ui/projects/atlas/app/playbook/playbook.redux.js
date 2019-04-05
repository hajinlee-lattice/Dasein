import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";

var CONST = {
    FETCH_PLAY: 'FETCH_PLAY',
    FETCH_PLAYS: 'FETCH_PLAYS',
    FETCH_CONNECTIONS: 'FETCH_CONNECTIONS'
}

const initialState = {
    play: {},
    plays: [],
    connections: []
};

export const actions = {
    fetchPlay: (play_name, deferred) => dispatch => {
        deferred = deferred || { resolve: (data) => data }
        let observer = new Observer(
            response => {
                httpService.unsubscribeObservable(observer);
                dispatch({
                    type: CONST.FETCH_PLAY,
                    payload: response.data
                });
                return deferred.resolve(response.data);
            }
        );
        httpService.get('/pls/play/' + play_name, observer, {});
    },
    fetchPlays: (deferred) => dispatch => {
        deferred = deferred || { resolve: (data) => data }
        let observer = new Observer(
            response => {
                httpService.unsubscribeObservable(observer);
                dispatch({
                    type: CONST.FETCH_PLAYS,
                    payload: response.data
                });
                return deferred.resolve(response.data);
            }
        );
        httpService.get('/pls/play', observer, {});
    },
    fetchConnections: (play_name, deferred) => dispatch => {
        deferred = deferred || { resolve: (data) => data }
        let observer = new Observer(
            response => {
                httpService.unsubscribeObservable(observer);
                dispatch({
                    type: CONST.FETCH_CONNECTIONS,
                    payload: response.data
                });
                return deferred.resolve(response.data);
            }
        );
        httpService.get(`/pls/play/${play_name}/launches/configurations`, observer, {});
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
        default:
            return state;
    }
};