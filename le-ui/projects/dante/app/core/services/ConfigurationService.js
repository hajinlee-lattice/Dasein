angular.module('mainApp.core.services.ConfigurationService', [
    'common.utilities.browserstorage',
    'mainApp.core.utilities.ServiceErrorUtility'
])
.service('ConfigurationService', function ($http, $q, BrowserStorageUtility, ServiceErrorUtility) {
    
    this.GetConfiguration = function () {
        var deferred = $q.defer();
        
        var previousMetadata = BrowserStorageUtility.getMetadata();
        var previousWidgetConfig = BrowserStorageUtility.getWidgetConfig();
        if (previousMetadata != null && previousMetadata.Timestamp > new Date().getTime() &&
            previousWidgetConfig != null && previousWidgetConfig.Timestamp > new Date().getTime()) {
            var cachedResponse = {
                success: true,
                resultObj: null,
                resultErrors: null
            };
            deferred.resolve(cachedResponse);
        } else {
            $http({
                method: "GET",
                url: "/ulysses/danteconfiguration"
                //url: "./DanteService.svc/GetConfiguration"
            })
            .success(function(data, status, headers, config) {
                if (data == null) {
                    deferred.resolve(ServiceErrorUtility.HandleFriendlyNoResponseFailure());
                }
                
                var result = {
                    success: data.Success,
                    resultObj: null,
                    resultErrors: null
                };
                if (data.Success === true) {
                    if(data.Result != null && data.Result.MetadataDocument != null) {
                        BrowserStorageUtility.setMetadata(data.Result.MetadataDocument);
                    }

                    if(data.Result != null && data.Result.WidgetConfigurationDocument != null) {
                        BrowserStorageUtility.setWidgetConfig(JSON.parse(data.Result.WidgetConfigurationDocument));
                    }
                } else {
                    result.resultErrors = ServiceErrorUtility.HandleFriendlyServiceResponseErrors(data);
                }
                
                deferred.resolve(result);
            })
            .error(function(data, status, headers, config) {
                deferred.resolve(ServiceErrorUtility.HandleFriendlyNoResponseFailure());
            });
        }
        
        return deferred.promise;
    };
});