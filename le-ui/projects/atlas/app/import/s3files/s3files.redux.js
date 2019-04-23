import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
import {store} from 'store';

var CONST = {
    FETCH_TEMPLATES: 'FETCH_TEMPLATES'
};
const initialState = {
    templates: []
};

export const actions = {
    fetchTemplates: () => {
        let observer = new Observer(
            response => {
                httpService.unsubscribeObservable(observer);
                return store.dispatch({
                    type: CONST.FETCH_TEMPLATES,
                    payload: response.data
                });
            }
        );
        httpService.get('/pls/cdl/s3import/fileList', observer, {});
    }
};

export const reducer = (state = initialState, action) => {
    switch (action.type) {
        case CONST.FETCH_TEMPLATES:
            return {
                ...state,
                templates: action.payload
            };
        default:
            return state;
    }
};