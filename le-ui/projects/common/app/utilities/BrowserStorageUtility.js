angular.module('common.utilities.browserstorage', [])                                                                                                                                                                        
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

    this._featureFlagsDocumentStorageKey = "GriotFeatureFlagsDocument";
    this._featureFlagsDocument = null; // actual client session object
    
    this._sessionLastActiveTimestampStorageKey = "GriotSessionLastActiveTimestamp";
    this._sessionShouldShowJobCompleteMessage = "GriotSessionShowJobCompleteMessage";

    this._OAuthAccessTokenStorageKey = "GriotOAuthAccessToken";
    this._OAuthAccessToken = null; // actual client session object

    this.setSessionLastActiveTimestamp = function(timeStamp) {
        $.jStorage.set(this._sessionLastActiveTimestampStorageKey, timeStamp);
    };
    
    this.getSessionLastActiveTimestamp = function() {
        $.jStorage.reInit();
        return $.jStorage.get(this._sessionLastActiveTimestampStorageKey);
    };
    
    this.setSessionShouldShowJobCompleteMessage = function(shouldShow) {
        $.jStorage.set(this._sessionShouldShowJobCompleteMessage, shouldShow);
    };
    
    this.getSessionShouldShowJobCompleteMessage = function() {
        return $.jStorage.get(this._sessionShouldShowJobCompleteMessage);
    };
    
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

    this.setFeatureFlagsDocument = function (data, successHandler) {
        this._setProperty(data, successHandler, "_featureFlagsDocument", "_featureFlagsDocumentStorageKey");
    };

    this.getFeatureFlagsDocument = function () {
        var featureFlags = this._getProperty("_featureFlagsDocument", "_featureFlagsDocumentStorageKey");
        return featureFlags || {};
    };

    this.setSessionDocument = function (data, successHandler) {
        this._setProperty(data, successHandler, "_sessionDocument", "_sessionDocumentStorageKey");
    };

    this.getSessionDocument = function () {
        return this._getProperty("_sessionDocument", "_sessionDocumentStorageKey");
    };

    this.clearSessionDocument = function (successHandler) {
        this._setProperty(null, successHandler, "_sessionDocument", "_sessionDocumentStorageKey");
    };

    this.setClientSession = function (data, successHandler) {
        if (data != null && data.AvailableRights != null) {
            var availableRightsDictionary = {};
            
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

    this.clearClientSession = function (successHandler) {
        this._setProperty(null, successHandler, "_clientSession", "_clientSessionStorageKey");
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

    this.setOAuthAccessToken = function (data, successHandler) {
        this._setProperty(data, successHandler, "_OAuthAccessToken", "_OAuthAccessTokenStorageKey");
    };

    this.getOAuthAccessToken = function () {
        return this._getProperty("_OAuthAccessToken", "_OAuthAccessTokenStorageKey");
    };

    this.clearOAuthAccessToken = function (successHandler) {
        this._setProperty(null, successHandler, "_OAuthAccessToken", "_OAuthAccessTokenStorageKey");
    };

    // Helper method to set a property
    // by adding it to local storage and then calling a success handler.
    this._setProperty = function (data, successHandler, propStorageObjName, propStorageKeyName) {
        if (this[propStorageKeyName]) {
            if (data != null && typeof data != 'string') {
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
    };

    // Helper method to get a property
    // by grabbing it from local storage.
    this._getProperty = function (propStorageObjName, propStorageKeyName) {
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
    };

    this.getHistory = function(username, currentTenant) {
        var LatticeSettings = $.jStorage.get("LatticeSettings"),
            username = username || '',
            currentTenant = currentTenant || '',
            values = {},
            result = [],
            history = [],
            login, tenants, all;

        if (LatticeSettings) {
            login = LatticeSettings.Login || {}; 
            tenants = login.TenantsList || {};
            all = tenants.History || {};
            history = typeof all[username] == 'object' ? all[username] : [];
        }

        for (var i=0, item, val, exists; i<history.length; i++) {
            item = history[i];
            val = item['DisplayName'];
            exists = values[val];

            values[val] = true;

            if (!exists && (!currentTenant || item['DisplayName'] != currentTenant)) {
                result.push(item);
            }
        }

        return result;
    }
    
    this.setHistory = function(username, tenant) {
        if (!username) {
            return;
        }

        var LatticeSettings = $.jStorage.get("LatticeSettings") || {};

        if (!LatticeSettings.Login) {
            LatticeSettings.Login = {};
        }

        if (!LatticeSettings.Login.TenantsList) {
            LatticeSettings.Login.TenantsList = {};
        }

        if (!LatticeSettings.Login.TenantsList.History) {
            LatticeSettings.Login.TenantsList.History = {};
        }

        if (!tenant) {
            LatticeSettings.Login.TenantsList.History[username] = [];
        } else if (!LatticeSettings.Login.TenantsList.History[username]) {
            LatticeSettings.Login.TenantsList.History[username] = [ tenant ];
        } else {
            LatticeSettings.Login.TenantsList.History[username].unshift(tenant);
        }

        $.jStorage.set("LatticeSettings", LatticeSettings);
    }
    
    this.clearHistory = function(username) {
        this.setHistory(username);
    }

    //This method will be used to clear out stored data on logout 
    //and possibly reset system cache
    this.clear = function(keepAuthentication) {
        var ClientSession, LatticeSettings;

        keepAuthentication = typeof keepAuthentication === 'boolean' 
            ? keepAuthentication 
            : false;

        if (keepAuthentication) {
            ClientSession = this.getClientSession();
        }

        // cross-application cross-login settings
        LatticeSettings = $.jStorage.get("LatticeSettings");

        $.jStorage.flush();

        if (keepAuthentication) {
            this.setClientSession(ClientSession);
        }

        $.jStorage.set("LatticeSettings", LatticeSettings);
    };
});