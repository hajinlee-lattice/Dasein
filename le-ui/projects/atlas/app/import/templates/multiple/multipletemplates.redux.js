import httpService from 'common/app/http/http-service';
import Observer from 'common/app/http/observer';
import { store } from 'store';

var CONST = {
    FETCH_TEMPLATES: 'FETCH_TEMPLATES',
    FETCH_PRIORITIES: 'FETCH_PRIORITIES',
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
            // httpService.unsubscribeObservable(observer)
            return store.dispatch({
                type: CONST.FETCH_PRIORITIES,
                payload: response.data,
            });
        });
        setTimeout(() => {
            observer.next({
                data: [
                    'System A',
                    'System C',
                    'System D',
                    'System Z',
                    'System F',
                    'System B',
                    'System U',
                ],
            });
        }, 3000);
        // httpService.get('/pls/cdl/s3import/template', observer, {})
    },
    resetPriorities: () => {
        return store.dispatch({
            type: CONST.RESET_PRIORITIES,
            payload: [],
        });
    },
    savePriorities: (newList) => {
        let observer = new Observer(response => {
            // httpService.unsubscribeObservable(observer);
            return store.dispatch({
                type: CONST.SAVE_PRIORITIES,
                payload: { saved: true },
            });
        });
        setTimeout(() => {
            observer.next();
        }, 3000);
       
        // httpService.get('/pls/cdl/s3import/template', observer, {});
    },
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
        default:
            return state;
    }
};
