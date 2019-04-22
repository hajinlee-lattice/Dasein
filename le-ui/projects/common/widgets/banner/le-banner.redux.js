import {TYPE_ERROR, TYPE_INFO, TYPE_SUCCESS, TYPE_WARNING} from '../banner/le-banner.utils';
const CONST = {
    REFRESH_VIEW: 'REFRESH_VIEW',
    OPEN_BANNER: 'OPEN_BANNER',
    CLOSE_BANNER: 'CLOSE_BANNER'
};

const initialState = {
    open: false,
    config: {}
};

export const actions = {

    error: (store, config = {}) => {
        config.type = TYPE_ERROR;
        return store.dispatch({
            type: CONST.OPEN_BANNER,
            payload: {
                open: true,
                config: config
            }
        })
    },
    info: (store, config = {}) => {
        config.type = TYPE_INFO;
        return store.dispatch({
            type: CONST.OPEN_BANNER,
            payload: {
                open: true,
                config: config
            }
        })
    },

    success: (store, config = {}) => {
        config.type = TYPE_SUCCESS;
        return store.dispatch({
            type: CONST.OPEN_BANNER,
            payload: {
                open: true,
                config: config
            }
        })
    },
    warning: (store, config = {}) => {
        config.type = TYPE_WARNING;
        return store.dispatch({
            type: CONST.OPEN_BANNER,
            payload: {
                open: true,
                config: config
            }
        })
    },

    refreshView: (store, config = {}) => {
        return store.dispatch({
            type: CONST.REFRESH_VIEW,
            payload: {
                open: true,
                config: config
            }
        })
    },

    openBanner: (store, config = {}) => {
        return store.dispatch({
            type: CONST.OPEN_BANNER,
            payload: {
                open: true,
                config: config
            }
        })
    },

    closeBanner: (store) => {
        return store.dispatch({
            type: CONST.CLOSE_BANNER,
            payload: {
                open: false,
                config: {}
            }
        })
    }
};

export const reducer = (state = initialState, action) => {
    switch (action.type) {
        case CONST.OPEN_BANNER:
            return {
                open: action.payload.open,
                config: action.payload.config
            };
        case CONST.CLOSE_BANNER:
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