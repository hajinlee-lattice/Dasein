// import {store} from 'store';

var CONST = {
    TOGGLE_MODAL: 'TOGGLE_MODAL'
};
const initialState = {
    opened: false,
    callback: undefined,
    template: undefined
};

export const actions = {
    toggleModal: (store, callback, template) => {
        console.log(store, callback, template);
        return store.dispatch({
            type: CONST.TOGGLE_MODAL,
            payload: {
                callbackFn: callback,
                templateFn: template
            }
        })
    }
};

export const reducer = (state = initialState, action) => {
    switch (action.type) {
        case CONST.TOGGLE_MODAL:
            return {
                opened: !state.opened,
                templateFn: action.payload.templateFn,
                callbackFn: action.payload.callbackFn
            };
        default:
            return state;
    }
};