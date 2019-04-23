import { TYPE_ERROR, TYPE_INFO, TYPE_SUCCESS, TYPE_WARNING } from '../banner/le-banner.utils';
const CONST = {
    REFRESH_VIEW: 'REFRESH_VIEW',
    OPEN_BANNER: 'OPEN_BANNER',
    CLOSE_BANNER: 'CLOSE_BANNER',
    CLEAR_BANNERS: 'CLEAR_BANNERS'
};

const initialState = {
    bannersList: [],
    open: false,
    config: {}
};

export const actions = {

    error: (store, config = {}) => {
        config.type = TYPE_ERROR;
        actions.openBanner(store, config);
    },
    info: (store, config = {}) => {
        config.type = TYPE_INFO;
        actions.openBanner(store, config);
    },

    success: (store, config = {}) => {
        config.type = TYPE_SUCCESS;
        actions.openBanner(store, config);
    },
    warning: (store, config = {}) => {
        config.type = TYPE_WARNING;
        actions.openBanner(store, config);
    },

    openBanner: (store, config = {}) => {
        let bannerStore = store.getState('le-banner')['le-banner'];
        let list = [];
        if(bannerStore.bannersList){
            list = [...bannerStore.bannersList];
        }
        let alreadyAdded = false;
        list.forEach(conf => {
            if(conf.title == config.title){
                conf.count = conf.count + 1;
                alreadyAdded = true;
                return;
            }
        });
        if(!alreadyAdded){
            config.count = 1;
            list.push(config);
        }
        return store.dispatch({
            type: CONST.OPEN_BANNER,
            payload: {
                bannersList: list
            }
        })
    },

    closeBanner: (store, banner) => {
        let bannerStore = store.getState('le-banner')['le-banner'];
        let list = bannerStore.bannersList || [];
        list.forEach((element, index) => {
            if (element.title == banner.title) {
                list.splice(index, 1);
            }
        });
        return store.dispatch({
            type: CONST.CLOSE_BANNER,
            payload: {
                open: false,
                config: {},
                bannersList: list
            }
        })
    },
    clearBanners: (store) => {
        return store.dispatch({
            type: CONST.CLEAR_BANNERS,
            payload: {
                open: false,
                config: {},
                bannersList: []
            }
        })
    }
};

export const reducer = (state = initialState, action) => {
    switch (action.type) {
        case CONST.OPEN_BANNER:
            return {
                open: action.payload.open,
                config: action.payload.config,
                bannersList: action.payload.bannersList
            };
        case CONST.CLOSE_BANNER:
            return {
                open: action.payload.open,
                config: action.payload.config,
                bannersList: action.payload.bannersList
            };
        
        case CONST.CLEAR_BANNERS:
            return {
                open: action.payload.open,
                config: action.payload.config,
                bannersList: action.payload.bannersList
            };
        default:
            return state;
    }
};