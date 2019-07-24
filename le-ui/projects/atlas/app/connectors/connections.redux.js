import httpService from "common/app/http/http-service";
import Observer from "common/app/http/observer";
import {store} from 'store';

var CONST = {
    USER_ID: 'USER_ID',
    USER_DOCUMENT: 'USER_DOCUMENT',
    AUTH_CODE: 'AUTHORIZATION_CODE',
    EXTERNAL_INTEGRATION_ENABLED: 'EXTERNAL_INTEGRATION_ENABLED'
};
const initialState = {
    templates: []
};

export const actions = {
    fetchUserDocument: () => {
        let observer = new Observer(
            response => {
                if (response.data) {
                    httpService.unsubscribeObservable(observer);
                    console.log(response.data);
                    return store.dispatch({
                        type: CONST.USER_DOCUMENT,
                        userName: response.data.name,
                        userId: response.data.id,
                        accessToken: response.data.accessToken
                    });
                }
            }
        );

        // httpService.get(('/tray/user?userName=' + userName), observer);
        httpService.get('/tray/userdocument', observer);
    },
    setUserId: (userId) => {
        return store.dispatch({
         type: CONST.USER_ID,
         userId: userId
        });
    },
    setAuthorizationCode: (authorizationCode) => {
        return store.dispatch({
         type: CONST.AUTH_CODE,
         authorizationCode: authorizationCode
        });
    },
    setExternalIntegrationEnabled: (isExternalIntegrationEnabled) => {
        console.log(isExternalIntegrationEnabled);
        return store.dispatch({
            type: CONST.EXTERNAL_INTEGRATION_ENABLED,
            isExternalIntegrationEnabled: isExternalIntegrationEnabled
        });
    }
};

export const reducer = (state = {}, action) => {
    switch (action.type) {
        case CONST.AUTH_CODE:
            return Object.assign({}, state, { authorizationCode: action.authorizationCode });
        case CONST.USER_DOCUMENT:
            return Object.assign({}, state, { userName: action.userName, userId: action.userId, accessToken: action.accessToken });
        case CONST.USER_ID:
            return Object.assign({}, state, { userId: action.userId });
        case CONST.EXTERNAL_INTEGRATION_ENABLED:
            return Object.assign({}, state, { isExternalIntegrationEnabled: action.isExternalIntegrationEnabled });
        default:
            return state;
    }
};