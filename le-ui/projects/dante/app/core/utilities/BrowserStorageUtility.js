angular.module('common.utilities.browserstorage', [])                                                                                                                                                                        
.service('BrowserStorageUtility', function () {
    
    this.CacheTimeout = 1200000; // 20 minutes in milliseconds
        
    this._clientSessionStorageKey = "DanteClientSession";
    this._clientSession = null; // actual client session object
    
    this._resourceStringsStorageKey = "DanteResourceStrings";
    this._resourceStrings = null; // actual resource strings object

    this._currentRootObjectStorageKey = "DanteCurrentRootObject";
    this._currentRootObject = null; // current root object
    
    this._rootApplicationStorageKey = "DanteRootApplication";
    this._rootApplication = null; // current root application

    this._metadataStorageKey = "DanteMetadata";
    this._metadata = null;

    this._widgetConfigStorageKey = "DanteWidgetConfig"; //cwkTODO may have to change this to a list eventually
    this._widgetConfig = null;
    
    this._selectedCrmObjectStorageKey = "DanteSelectedCrmObject";
    this._selectedCrmObject = null; // current selected crm object
    
    this.crmCustomSettings = null;

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
    
    this.setResourceStrings = function (data, successHandler) {
        this._setProperty(data, successHandler, "_resourceStrings", "_resourceStringsStorageKey");
    };
    
    this.getResourceStrings = function () {
        return this._getProperty("_resourceStrings", "_resourceStringsStorageKey");
    };

    this.setCurrentRootObject = function (data, successHandler) {
        this._setProperty(data, successHandler, "_currentRootObject", "_currentRootObjectStorageKey");
    };
    
    this.getCurrentRootObject = function () {
        return this._getProperty("_currentRootObject", "_currentRootObjectStorageKey");
    };
    
    this.setRootApplication = function (data, successHandler) {
        this._setProperty(data, successHandler, "_rootApplication", "_rootApplicationStorageKey");
    };
    
    this.getRootApplication = function () {
        return this._getProperty("_rootApplication", "_rootApplicationStorageKey");
    };

    this.setMetadata = function (data, successHandler) {
        this._setProperty(data, successHandler, "_metadata", "_metadataStorageKey");
    };

    this.getMetadata = function () {
        return this._getProperty("_metadata", "_metadataStorageKey");
    };

    this.setWidgetConfig = function (data, successHandler) {
        this._setProperty(data, successHandler, "_widgetConfig", "_widgetConfigStorageKey");
    };

    this.getWidgetConfig = function () {
        return this._getProperty("_widgetConfig", "_widgetConfigStorageKey");
    };
    
    this.setSelectedCrmObject = function (data, successHandler) {
        this._setProperty(data, successHandler, "_selectedCrmObject", "_selectedCrmObjectStorageKey");
    };

    this.getSelectedCrmObject = function () {
        return this._getProperty("_selectedCrmObject", "_selectedCrmObjectStorageKey");
    };
    
    this.setCrmCustomSettings = function (data) {
        this.crmCustomSettings = data;
    };
    
    this.getCrmCustomSettings = function () {
        return this.crmCustomSettings;
    };

    // Helper method to set a property
    // by adding it to local storage and then calling a success handler.
    this._setProperty = function (data, successHandler, propStorageObjName, propStorageKeyName) {
        if (this[propStorageKeyName]) {
            // Set a Timestamp so we can check if the cached data is still valid
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
    this.clear = function (keepAuthentication) {
        keepAuthentication = typeof keepAuthentication === 'boolean' ? keepAuthentication : false;
        if(!keepAuthentication) {
           this.setClientSession(null);
        }
        this.setCurrentRootObject(null);
        this.setWidgetConfig(null);
        this.setMetadata(null);
        this.setRootApplication(null);
        this.setResourceStrings(null);
    };
});