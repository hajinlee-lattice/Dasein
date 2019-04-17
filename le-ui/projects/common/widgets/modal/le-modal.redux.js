const CONST = {
    REFRESH_VIEW: 'REFRESH_VIEW',
    OPEN_MODAL: 'OPEN_MODAL',
    CLOSE_MODAL: 'CLOSE_MODAL'
};
const initialState = {
    open: false,
    config : {}
};

export const actions = {

    refreshView: (store, config) => {
        return store.dispatch({
            type: CONST.REFRESH_VIEW,
            payload: {
                open: true,
                config: config
            }
        })
    },

    openModal: (store, config) => {
        return store.dispatch({
            type: CONST.OPEN_MODAL,
            payload: {
                open: true,
                config: config
            }
        })
    },

    closeModal: (store) => {
        return store.dispatch({
            type: CONST.CLOSE_MODAL,
            payload: {
                open: false,
                config : {}
            }
        })
    }
};

export const reducer = (state = initialState, action) => {
    switch (action.type) {
        case CONST.OPEN_MODAL:
            return {
                open: action.payload.open,
                config: action.payload.config
            };
        case CONST.CLOSE_MODAL:
            return {
                open: action.payload.open,
                config: action.payload.config
            };
        case CONST.REFRESH_VIEW:
            return {
                open: action.payload.open,
                config: action.payload.config
            };
        default:
            return state;
    }
};