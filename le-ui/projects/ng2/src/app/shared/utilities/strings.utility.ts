import { Injectable } from '@angular/core';
declare var $:any;

@Injectable()
export class StringsUtility {
    
    resourceStringsInitialized: boolean = false;
    DefaultLocale: string = "en-US";
    configStringsStorageKey: string = "ConfigStrings";
    configStringsKeyValueStorageKey: string = "ConfigStringKeyValues";
    configStringsLocaleStorageKey: string = "ConfigStringsLocale";
    configStrings: any;
    keyValuePairs;

    constructor() { 
        this.configStrings = {};
        this.keyValuePairs = [];
    }

    // get a config resource string value given its key
    public getString(key, replacements?) {
        if (this.configStrings == null) {
            this.populateConfigStrings();
        }
        var toReturn = this.configStrings[key] || key;
        if (replacements) {
            toReturn = this.replaceTokens(toReturn, replacements);
        }
        return toReturn;
    };
    
    public setStrings(strings) {
        Object.keys(strings).forEach(key => this.configStrings[key] = strings[key])
    }

    clearResourceStrings() {
        $.jStorage.set(this.configStringsStorageKey, null);
        $.jStorage.set(this.configStringsKeyValueStorageKey, null);
        $.jStorage.set(this.configStringsLocaleStorageKey, null);
    };
    
    getResourceStrings() {
        return $.jStorage.get(this.configStringsStorageKey);
    };
    
    getResourceStringKeyValues() {
        return $.jStorage.get(this.configStringsKeyValueStorageKey);
    };
    
    setCurrentLocale(localeName) {
        $.jStorage.set(this.configStringsLocaleStorageKey, localeName);
    };
    
    getCurrentLocale() {
        return $.jStorage.get(this.configStringsLocaleStorageKey);
    };

    replaceTokens(key, replacements) {
        for (var i = 0; i < replacements.length; i++) {
            while (key.indexOf("{" + i + "}") != -1) {
                key = key.replace("{"+i+"}", replacements[i]);
            }
        }
        return key;
    };
    // populate in memory config resource strings dictionary from local storage
    populateConfigStrings() {
        var configStringsFromStorage = $.jStorage.get(
            this.configStringsStorageKey);

        this.configStrings = configStringsFromStorage || {};
    };

    // store a list of config resource string objects in local storage
    // set the config resource strings, e.g. if updating a subset for another locale
    // data should be a list of objects with "key" and a "value" properties
    storeStringList(stringList, successHandler) {
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
    PreloadImage(el, callback) {
        var img = new Image();
        img.onload = function() {
            callback(img);
        };
        img.src = el.attr("src");
    };

    SetBrowserTitle() {
        //document.title = this.getString("BROWSER_TITLE");
    };

}
