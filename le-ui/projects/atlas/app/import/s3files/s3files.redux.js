import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
import {store} from 'store';

var CONST = {
    FETCH_S3_FILES: 'FETCH_S3_FILES',
    SET_PATH: 'SET_PATH'
};
const initialState = {
    s3Files: [],
    path: ''
};

export const s3actions = {
    fetchS3Files: (path) => {
        let observer = new Observer(
            response => {
                httpService.unsubscribeObservable(observer);
                return store.dispatch({
                    type: CONST.FETCH_S3_FILES,
                    payload: response.data
                });
            }
        );
        httpService.get(`/pls/cdl/s3import/fileList?s3Path=${path}`, observer, {});
    },
    setPath: (path) => {
        return store.dispatch({
            type: CONST.SET_PATH,
            payload: path
        });
    }
};

export const s3reducer = (state = initialState, action) => {
    switch (action.type) {
        case CONST.FETCH_S3_FILES:
            return {
                ...state,
                s3Files: action.payload
            };
        case CONST.SET_PATH:
            return {
                ...state,
                path: action.payload
            };
        default:
            return state;
    }
};