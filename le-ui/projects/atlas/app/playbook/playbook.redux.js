import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";

const SET_PLAY = 'SET_PLAY';
const FETCH_PLAY = 'FETCH_PLAY';
const FETCH_PLAYS = 'FETCH_PLAYS';

const host = '/pls'; //default

const initialState = { play: null };

export const actions = {
    setPlay: () => dispatch => {
        return dispatch({
            type: SET_PLAY,
            payload: initialState.play
        });
    },
    fetchPlay: (play_name) => dispatch => {
        let observer = new Observer(
            response => {
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
    }
    ///pls/play/{playname}/launches/configurations
};

export const reducer = (state = initialState, action) => {
    switch (action.type) {
        case SET_PLAY:
            return {
                ...state,
                play: action.payload
            }
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
        default:
            return state;
    }
};