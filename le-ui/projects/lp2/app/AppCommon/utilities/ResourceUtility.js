angular.module('mainApp.appCommon.utilities.ResourceUtility', [])                                                                                                                                                                        
.service('ResourceUtility', function () {
    
    this.DefaultLocale = "en-US";
    
    // key for config resource string object in local storage
    this.configStringsStorageKey = "ConfigStrings";
    this.configStringsKeyValueStorageKey = "ConfigStringKeyValues";
    this.configStringsLocaleStorageKey = "ConfigStringsLocale";

    // in memory dictionary of config resource string keys and values
    this.configStrings = null;
    
    // Indicator that somebody accessed the app without going through the proper channel
    this.resourceStringsInitialized = false;

    //This is needed for Flex so it can create it's StringLocator instance
    this.keyValuePairs = [];

    // get a config resource string value given its key
    this.getString = function (key, replacements) {
        if (this.configStrings == null) {
            this.populateConfigStrings();
        }
        var toReturn = this.configStrings[key] || key;
        if (replacements) {
            toReturn = this.replaceTokens(toReturn, replacements);
        }
        return toReturn;
    };
    
    this.clearResourceStrings = function () {
        $.jStorage.set(this.configStringsStorageKey, null);
        $.jStorage.set(this.configStringsKeyValueStorageKey, null);
        $.jStorage.set(this.configStringsLocaleStorageKey, null);
    };
    
    this.getResourceStrings = function () {
        return $.jStorage.get(this.configStringsStorageKey);
    };
    
    this.getResourceStringKeyValues = function () {
        return $.jStorage.get(this.configStringsKeyValueStorageKey);
    };
    
    this.setCurrentLocale = function (localeName) {
        $.jStorage.set(this.configStringsLocaleStorageKey, localeName);
    };
    
    this.getCurrentLocale = function () {
        return $.jStorage.get(this.configStringsLocaleStorageKey);
    };

    this.replaceTokens = function (key, replacements) {
        for (var i = 0; i < replacements.length; i++) {
            while (key.indexOf("{" + i + "}") != -1) {
                key = key.replace("{"+i+"}", replacements[i]);
            }
        }
        return key;
    };
    // populate in memory config resource strings dictionary from local storage
    this.populateConfigStrings = function () {
        var configStringsFromStorage = $.jStorage.get(
            this.configStringsStorageKey);

        this.configStrings = configStringsFromStorage || {};
    };

    // store a list of config resource string objects in local storage
    // set the config resource strings, e.g. if updating a subset for another locale
    // data should be a list of objects with "key" and a "value" properties
    this.storeStringList = function (stringList, successHandler) {
        //check if there is already anything in memory
        if (this.configStrings == null) {
            this.populateConfigStrings();
        }

        if (stringList != null) {
            for (var i = 0; i < stringList.length; i++) {
                var curPair = stringList[i];
                var curKey = curPair.Key;
                var curValue = curPair.Value;

                this.keyValuePairs.push(curPair);
                this.configStrings[curKey] = curValue;
            }
            this.resourceStringsInitialized = true;
        }

        // store updated set of strings back into local storage
        $.jStorage.set(this.configStringsStorageKey,
            this.configStrings);
            
        $.jStorage.set(this.configStringsKeyValueStorageKey,
            this.keyValuePairs);

        $(document).trigger('configStringsLoadedEvent');

        if (successHandler != null) {
            successHandler();
        }
    };

    //Preloads an image and return the preloaded image object
    this.PreloadImage = function (el, callback) {
        var img = new Image();
        img.onload = function () {
            callback(img);
        };
        img.src = el.attr("src");
    };

    this.SetBrowserTitle = function () {
        document.title = this.getString("BROWSER_TITLE");
    };
});