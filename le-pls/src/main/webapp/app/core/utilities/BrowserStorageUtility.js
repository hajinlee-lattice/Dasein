angular.module('mainApp.core.utilities.BrowserStorageUtility', [])                                                                                                                                                                        
.service('BrowserStorageUtility', function () {
    
    this.CacheTimeout = 28800000; // 8 hours in Milliseconds

    this._tokenDocumentStorageKey = "GriotTokenDocument";
    this._tokenDocument = null; // document containing the authentication token
    
    this._loginDocumentStorageKey = "GriotLoginDocument";
    this._loginDocument = null; // document containing basic information about the Bard deployment
    
    this._sessionDocumentStorageKey = "GriotSessionDocument";
    this._sessionDocument = null; // actual session object

    this._clientSessionStorageKey = "GriotClientSession";
    this._clientSession = null; // actual client session object

    this._currentTabStorageKey = "GriotCurrentTab";
    this._currentTab = null; // currently selected tab in the main header
    
    this._configDocumentStorageKey = "GriotConfigDocument";
    this._configDocument = null; // actual client session object
    
    this._widgetConfigDocumentStorageKey = "GriotWidgetConfigDocument";
    this._widgetConfigDocument = null; // actual client session object
    
    this.setTokenDocument = function (data, successHandler) {
        this._setProperty(data, successHandler, "_tokenDocument", "_tokenDocumentStorageKey");
    };

    this.getTokenDocument = function () {
        return this._getProperty("_tokenDocument", "_tokenDocumentStorageKey");
    };

    this.setLoginDocument = function (data, successHandler) {
        this._setProperty(data, successHandler, "_loginDocument", "_loginDocumentStorageKey");
    };

    this.getLoginDocument = function () {
        return this._getProperty("_loginDocument", "_loginDocumentStorageKey");
    };
    
    this.setSessionDocument = function (data, successHandler) {
        this._setProperty(data, successHandler, "_sessionDocument", "_sessionDocumentStorageKey");
    };

    this.getSessionDocument = function () {
        return this._getProperty("_sessionDocument", "_sessionDocumentStorageKey");
    };

    this.setClientSession = function (data, successHandler) {
        var availableRightsDictionary = {};
        if (data != null && data.AvailableRights != null) {
            $.each(data.AvailableRights, function(key, availableRight) {
                        availableRightsDictionary[availableRight.Key] = availableRight.Value;
                    });
            data.availableRightsDictionary= availableRightsDictionary;
        }

        this._setProperty(data, successHandler, "_clientSession", "_clientSessionStorageKey");
    };

    this.getClientSession = function () {
        return this._getProperty("_clientSession", "_clientSessionStorageKey");
    };

    this.setCurrentTab = function (data, successHandler) {
        this._setProperty(data, successHandler, "_currentTab", "_currentTabStorageKey");
    };
    
    this.getCurrentTab = function () {
        return this._getProperty("_currentTab", "_currentTabStorageKey");
    };
    
    this.setConfigDocument = function (data, successHandler) {
        this._setProperty(data, successHandler, "_configDocument", "_configDocumentStorageKey");
    };

    this.getConfigDocument = function () {
        return this._getProperty("_configDocument", "_configDocumentStorageKey");
    };
    
    this.setWidgetConfigDocument = function (data, successHandler) {
        this._setProperty(data, successHandler, "_widgetConfigDocument", "_widgetConfigDocumentStorageKey");
    };

    this.getWidgetConfigDocument = function () {
        return this._getProperty("_widgetConfigDocument", "_widgetConfigDocumentStorageKey");
    };
        // Helper method to set a property
    // by adding it to local storage and then calling a success handler.
    this._setProperty = function (data, successHandler, propStorageObjName, propStorageKeyName) {
        if (this[propStorageKeyName]) {
            if (data != null) {
                data.Timestamp = new Date().getTime() + this.CacheTimeout;
            }
            $.jStorage.set(this[propStorageKeyName], data);
            this[propStorageObjName] = data;
            if (successHandler && typeof(successHandler) === "function") {
                successHandler();
            }
        }
    };

    // Helper method to get a property
    // by grabbing it from local storage.
    this._getProperty = function (propStorageObjName, propStorageKeyName) {
        if (propStorageObjName && propStorageKeyName) {
            if (this[propStorageObjName] == null) {
                var fromStorage = $.jStorage.get(this[propStorageKeyName]);
                this[propStorageObjName] = fromStorage || null;
            }
            return this[propStorageObjName];
        } else {
            return null;
        }
    };

    //This method will be used to clear out stored data on logout 
    //and possibly reset system cache
    this.clear = function(keepAuthentication) {
        keepAuthentication = typeof keepAuthentication === 'boolean' ? keepAuthentication : false;
        if(!keepAuthentication) {
           this.setClientSession(null);
        }
        this.setLoginDocument(null);
        this.setSessionDocument(null);
        this.setCurrentTab(null);
        this.setConfigDocument(null);
        this.setWidgetConfigDocument(null);
    };
});