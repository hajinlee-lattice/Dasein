import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";

//const SET_PLAY = 'SET_PLAY';
const FETCH_PLAY = 'FETCH_PLAY';
const FETCH_PLAYS = 'FETCH_PLAYS';
//const SET_CONNECTIONS = 'SET_CONNECTIONS';
const FETCH_CONNECTIONS = 'FETCH_CONNECTIONS';

const host = '/pls'; //default

const initialState = {
    play: null,
    connections: null
 };

export const actions = {
    // setPlay: (play) => dispatch => {
    //     dispatch({
    //         type: SET_PLAY,
    //         payload: play
    //     });
    // },
    fetchPlay: (play_name) => dispatch => {
        let observer = new Observer(
            response => {
                httpService.unsubscribeObservable(observer);
                if (response.status == 200) {
                    return dispatch({
                        type: FETCH_PLAY,
                        payload: response.data
                    });
                }
            },
            error => { }
        );
        httpService.get(host + '/play/' + play_name, observer, {});
    },
    fetchPlays: () => dispatch => {
        let observer = new Observer(
            response => {
                httpService.unsubscribeObservable(observer);
                if (response.status == 200) {
                    return dispatch({
                        type: FETCH_PLAYS,
                        payload: response.data
                    });
                }
            },
            error => { }
        );
        httpService.get(host + '/play', observer, {});
    },
    // setConnections: (connections) => dispatch => {
    //     dispatch({
    //         type: SET_CONNECTIONS,
    //         payload: connections
    //     });
    // },
    fetchConnections: (play_name) => dispatch => {
        console.log('fetchConnections:');
        let observer = new Observer(
            response => {
                httpService.unsubscribeObservable(observer);
                if (response.status == 200) {
                    return dispatch({
                        type: FETCH_CONNECTIONS,
                        payload: response.data
                    });
                }
            },
            error => { }
        );
        httpService.get(host + `/play/${play_name}/launches/configurations`, observer, {});
    }
};

export const reducer = (state = initialState, action) => {
    switch (action.type) {
        // case SET_PLAY:
        //     return {
        //         ...state,
        //         play: action.payload
        //     }
        case FETCH_PLAY:
            return {
                ...state,
                play: action.payload
            }
        case FETCH_PLAYS:
            return {
                ...state,
                plays: action.payload
            }
        // case SET_CONNECTIONS:
        //     return {
        //         ...state,
        //         connections: action.payload
        //     }
        case FETCH_CONNECTIONS:
            return {
                ...state,
                connections: action.payload
            }
        default:
            return state;
    }
};