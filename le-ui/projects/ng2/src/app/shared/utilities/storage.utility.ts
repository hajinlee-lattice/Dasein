import { Injectable } from '@angular/core';
declare var $:any;

@Injectable()
export class StorageUtility {
   
    CacheTimeout: number = 28800000; // 8 hours in Milliseconds

    _tokenDocumentStorageKey: string = "GriotTokenDocument";
    _tokenDocument: any = null; // document containing the authentication token
    
    _loginDocumentStorageKey: string = "GriotLoginDocument";
    _loginDocument: any = null; // document containing basic information about the Bard deployment
    
    _sessionDocumentStorageKey: string = "GriotSessionDocument";
    _sessionDocument: any = null; // actual session object

    _clientSessionStorageKey: string = "GriotClientSession";
    _clientSession: any = null; // actual client session object

    _currentTabStorageKey: string = "GriotCurrentTab";
    _currentTab: any = null; // currently selected tab in the main header
    
    _configDocumentStorageKey: string = "GriotConfigDocument";
    _configDocument: any = null; // actual client session object
    
    _widgetConfigDocumentStorageKey: string = "GriotWidgetConfigDocument";
    _widgetConfigDocument: any = null; // actual client session object

    _featureFlagsDocumentStorageKey: string = "GriotFeatureFlagsDocument";
    _featureFlagsDocument: any = null; // actual client session object
    
    _sessionLastActiveTimestampStorageKey: string = "GriotSessionLastActiveTimestamp";
    _sessionShouldShowJobCompleteMessage: string = "GriotSessionShowJobCompleteMessage";

    _OAuthAccessTokenStorageKey: string = "GriotOAuthAccessToken";
    _OAuthAccessToken: any = null; // actual client session object

    constructor() { }

    setSessionLastActiveTimestamp(timeStamp) {
        $.jStorage.set(this._sessionLastActiveTimestampStorageKey, timeStamp);
    }
    
    getSessionLastActiveTimestamp() {
        $.jStorage.reInit();
        return $.jStorage.get(this._sessionLastActiveTimestampStorageKey);
    }
    
    setSessionShouldShowJobCompleteMessage(shouldShow?) {
        $.jStorage.set(this._sessionShouldShowJobCompleteMessage, shouldShow);
    }
    
    getSessionShouldShowJobCompleteMessage() {
        return $.jStorage.get(this._sessionShouldShowJobCompleteMessage);
    }
    
    setTokenDocument(data, successHandler?) {
        this._setProperty(data, successHandler, "_tokenDocument", "_tokenDocumentStorageKey");
    }

    getTokenDocument() {
        return this._getProperty("_tokenDocument", "_tokenDocumentStorageKey");
    }

    setLoginDocument(data, successHandler?) {
        console.log('setLoginDocument', data);
        this._setProperty(data, successHandler, "_loginDocument", "_loginDocumentStorageKey");
    }

    getLoginDocument() {
        console.log('getLoginDocument', this._getProperty("_loginDocument", "_loginDocumentStorageKey"));
        return this._getProperty("_loginDocument", "_loginDocumentStorageKey");
    }

    setFeatureFlagsDocument(data, successHandler?) {
        this._setProperty(data, successHandler, "_featureFlagsDocument", "_featureFlagsDocumentStorageKey");
    }

    getFeatureFlagsDocument() {
        var featureFlags = this._getProperty("_featureFlagsDocument", "_featureFlagsDocumentStorageKey");
        return featureFlags || {};
    }

    setSessionDocument(data, successHandler?) {
        this._setProperty(data, successHandler, "_sessionDocument", "_sessionDocumentStorageKey");
    }

    getSessionDocument() {
        return this._getProperty("_sessionDocument", "_sessionDocumentStorageKey");
    }

    clearSessionDocument(successHandler?) {
        this._setProperty(null, successHandler, "_sessionDocument", "_sessionDocumentStorageKey");
    }

    setClientSession(data, successHandler?) {
        if (data != null && data.AvailableRights != null) {
            var availableRightsDictionary = {};
            
            $.each(data.AvailableRights, function(key, availableRight) {
                availableRightsDictionary[availableRight.Key] = availableRight.Value;
            });
            
            data.availableRightsDictionary= availableRightsDictionary;
        }

        this._setProperty(data, successHandler, "_clientSession", "_clientSessionStorageKey");
    }

    getClientSession() {
        return this._getProperty("_clientSession", "_clientSessionStorageKey");
    }

    clearClientSession(successHandler?) {
        this._setProperty(null, successHandler, "_clientSession", "_clientSessionStorageKey");
    }

    setCurrentTab(data, successHandler?) {
        this._setProperty(data, successHandler, "_currentTab", "_currentTabStorageKey");
    }
    
    getCurrentTab() {
        return this._getProperty("_currentTab", "_currentTabStorageKey");
    }
    
    setConfigDocument(data, successHandler?) {
        this._setProperty(data, successHandler, "_configDocument", "_configDocumentStorageKey");
    }

    getConfigDocument() {
        return this._getProperty("_configDocument", "_configDocumentStorageKey");
    }
    
    setWidgetConfigDocument(data, successHandler?) {
        this._setProperty(data, successHandler, "_widgetConfigDocument", "_widgetConfigDocumentStorageKey");
    }

    getWidgetConfigDocument() {
        return this._getProperty("_widgetConfigDocument", "_widgetConfigDocumentStorageKey");
    }

    setOAuthAccessToken(data, successHandler?) {
        this._setProperty(data, successHandler, "_OAuthAccessToken", "_OAuthAccessTokenStorageKey");
    }

    getOAuthAccessToken() {
        return this._getProperty("_OAuthAccessToken", "_OAuthAccessTokenStorageKey");
    }

    clearOAuthAccessToken(successHandler?) {
        this._setProperty(null, successHandler, "_OAuthAccessToken", "_OAuthAccessTokenStorageKey");
    }

    // Helper method to set a property
    // by adding it to local storage and then calling a success handler.
    _setProperty(data, successHandler?, propStorageObjName?, propStorageKeyName?) {
        if (this[propStorageKeyName]) {
            if (data != null && typeof data == 'Object') {
                data.Timestamp = new Date().getTime() + this.CacheTimeout;
            } else {
                $.jStorage.deleteKey(this[propStorageKeyName]);
            }
            $.jStorage.set(this[propStorageKeyName], data);
            this[propStorageObjName] = data;
            if (successHandler && typeof(successHandler) === "function") {
                successHandler();
            }
        }
    }

    // Helper method to get a property
    // by grabbing it from local storage.
    _getProperty(propStorageObjName, propStorageKeyName) {
        if (propStorageObjName && propStorageKeyName) {
            if (this[propStorageObjName] == null) {
                $.jStorage.reInit();
                var fromStorage = $.jStorage.get(this[propStorageKeyName]);
                this[propStorageObjName] = fromStorage || null;
            }
            return this[propStorageObjName];
        } else {
            return null;
        }
    }

    //This method will be used to clear out stored data on logout 
    //and possibly reset system cache
    clear(keepAuthentication?) {
        keepAuthentication = typeof keepAuthentication === 'boolean' ? keepAuthentication : false;
        var clientSession;

        if (keepAuthentication) clientSession = this.getClientSession();

        $.jStorage.flush();

        if (keepAuthentication) {
            this.setClientSession(clientSession);
        }
    }

}
