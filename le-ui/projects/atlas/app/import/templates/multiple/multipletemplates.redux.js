import httpService from 'common/app/http/http-service';
import Observer from 'common/app/http/observer';
import { store } from 'store';

var CONST = {
    FETCH_TEMPLATES: 'FETCH_TEMPLATES',
    FETCH_PRIORITIES: 'FETCH_PRIORITIES',
    FETCH_SYSTEMS: 'FETCH_SYSTEMS',
    RESET_PRIORITIES: 'RESET_PRIORITIES',
    SAVE_PRIORITIES: 'SAVE_PRIORITIES',
};
const initialState = {
    templates: [],
    priorities: [],
};

export const actions = {
    fetchTemplates: () => {
        let observer = new Observer(response => {
            httpService.unsubscribeObservable(observer);
            return store.dispatch({
                type: CONST.FETCH_TEMPLATES,
                payload: response.data,
            });
        });
        httpService.get('/pls/cdl/s3import/template', observer, {});
    },
    fetchPriorities: () => {
        let observer = new Observer(response => {
            httpService.unsubscribeObservable(observer)
            return store.dispatch({
                type: CONST.FETCH_PRIORITIES,
                payload: response.data,
            });
        });
        httpService.get('/pls/cdl/s3import/system/list', observer, {})
    },
    resetPriorities: () => {
        return store.dispatch({
            type: CONST.RESET_PRIORITIES,
            payload: [],
        });
    },
    savePriorities: (newList) => {
        let observer = new Observer(response => {
            httpService.unsubscribeObservable(observer);
            return store.dispatch({
                type: CONST.SAVE_PRIORITIES,
                payload: { saved: true },
            });
        });
        httpService.post('../pls/cdl/s3import/system/list', newList, observer, {});
    },
    fetchSystems: (optionsObj) => {
        let observer = new Observer(response => {
            httpService.unsubscribeObservable(observer);
            return store.dispatch({
                type: CONST.FETCH_SYSTEMS,
                payload: response.data,
            });
        });
        let options = ''
        Object.keys(optionsObj).forEach((option, index) => {
            options = options.concat(`${option}${'='}${optionsObj[option]}`);
            if(index < Object.keys.length -1){
                options = options.concat('&');
            }
        });
        let url = `${'../pls/cdl/s3import/system/list?'}${options}`;
        httpService.get(url, observer, {});
    }
};

export const reducer = (state = initialState, action) => {
    switch (action.type) {
        case CONST.FETCH_TEMPLATES:
            return {
                ...state,
                templates: action.payload,
            };
        case CONST.FETCH_PRIORITIES:
            return {
                ...state,
                priorities: action.payload,
            };
        case CONST.RESET_PRIORITIES:
            return {
                ...state,
                priorities: action.payload,
            };
        case CONST.SAVE_PRIORITIES:
            return {
                ...state,
                saved: action.payload.saved,
            };
            case CONST.FETCH_SYSTEMS:
            return {
                ...state,
                systems: action.payload,
            };
        default:
            return state;
    }
};
