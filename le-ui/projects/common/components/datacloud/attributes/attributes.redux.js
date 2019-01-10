import axios from 'axios';

const FETCH_ATTRIBUTES = 'FETCH_ATTRIBUTES';
const CLEAR_ATTRIBUTES = 'CLEAR_ATTRIBUTES';
const initialState = { items: [] };

export const actions = {
    get: () => dispatch => {
        axios('https://jsonplaceholder.typicode.com/posts').then(response => {
            console.log(response);
            dispatch({
                type: FETCH_ATTRIBUTES,
                payload: response.data
            });
        });
    },
    clear: () => dispatch => {
        return dispatch({
            type: CLEAR_ATTRIBUTES
        });
    }
};

export const reducer = (state = initialState, action) => {
    switch (action.type) {
        case FETCH_ATTRIBUTES:
            return {
                ...state,
                items: action.payload
            };
        case CLEAR_ATTRIBUTES:
            return {
                ...state,
                items: []
            };
        default:
            return state;
    }
};
