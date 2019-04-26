import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
import {store} from 'store';

var CONST = {
    FETCH_S3_FILES: 'FETCH_S3_FILES'
};
const initialState = {
    s3Files: []
};

export const s3actions = {
    fetchS3Files: () => {
        let observer = new Observer(
            response => {
                httpService.unsubscribeObservable(observer);
                return store.dispatch({
                    type: CONST.FETCH_S3_FILES,
                    payload: response.data
                });
            }
        );
        httpService.get('/pls/cdl/s3import/fileList?s3Path=latticeengines-qa-customers/dropfolder/k9adsbgl/Templates/HierarchySchema/', observer, {});
    }

};

export const s3reducer = (state = initialState, action) => {
    switch (action.type) {
        case CONST.FETCH_S3_FILES:
            return {
                ...state,
                s3Files: action.payload
            };
        default:
            return state;
    }
};