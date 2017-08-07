angular.module('mainApp.core.services.ResourceStringsService', [
    'mainApp.appCommon.utilities.URLUtility',
    'mainApp.appCommon.utilities.ResourceUtility'
])
.service('ResourceStringsService', function ($http, $q, URLUtility, ResourceUtility) {
    
    this.DefaultLocale= "en-US";
    
    this.GetExternalResourceStringsForLocale = function (locale) {
        if (locale == null) {
            locale = this.DefaultLocale;
        }
        
        var webAddress = "assets/resources/" + locale + "/" + "ResourceStringsExternal.txt";
        return getResourceStringsAtWebAddress(webAddress);
    };

    this.GetInternalResourceStringsForLocale = function (locale) {
        if (locale == null) {
            locale = this.DefaultLocale;
        }
        
        var webAddress = "assets/resources/" + locale + "/" + "ResourceStrings.txt";
        return getResourceStringsAtWebAddress(webAddress);
    };

    function getResourceStringsAtWebAddress (webAddress) {
        
        var deferred = $q.defer();
        
        $http({
            method: 'GET', 
            url: webAddress
        })
        .success(function(data, status, headers, config) {
            if (data == null) return;
            
            var resourceStrings = {};
            var result = data.split("\r\n");
            for (var x=0;x<result.length;x++) {
                if (result[x] !== "") {
                    var resourceString = result[x].split("=");
                    resourceStrings[resourceString[0]] = resourceString[1];
                }
            }
            ResourceUtility.configStrings = resourceStrings;
            ResourceUtility.resourceStringsInitialized = true;
            deferred.resolve(true);
        })
        .error(function(data, status, headers, config) {
            deferred.resolve(true);
        });
        
        return deferred.promise;
    }
});