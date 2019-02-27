angular.module('mainApp.core.services.SessionService', [
    'mainApp.appCommon.utilities.URLUtility',
    'common.utilities.browserstorage',
    'mainApp.core.utilities.ServiceErrorUtility',
    'mainApp.appCommon.utilities.AuthenticationUtility'
])
.service('SessionService', function ($http, $q, URLUtility, BrowserStorageUtility, ServiceErrorUtility, AuthenticationUtility) {
    
    this.ValidateSession = function () {
        
        AuthenticationUtility.AppendHttpHeaders($http);
        
        var currentSessionId = URLUtility.CrmSessionID();
        var previousSessionId = BrowserStorageUtility.getClientSession();
        if (previousSessionId == null || previousSessionId != currentSessionId) {
            BrowserStorageUtility.clear(false);
        }
        
        var deferred = $q.defer();
        
        // $http({
        //     method: "GET", 
        //     url: "./DanteService.svc/ValidateSession"
        // })
        // .success(function(data, status, headers, config) {
        //     if (data == null) {
        //         deferred.resolve(ServiceErrorUtility.HandleFriendlyNoResponseFailure());
        //     }
            
        //     var result = {
        //         success: data.Success,
        //         resultObj: null,
        //         resultErrors: null
        //     };
        //     if (data.Success === true) {
        //         BrowserStorageUtility.setClientSession(currentSessionId);
        //         result.resultObj = data.Result;
        //     } else {
        //         result.resultErrors = ServiceErrorUtility.HandleFriendlyServiceResponseErrors(data);
        //     }
        // })
        // .error(function(data, status, headers, config) {
        //     deferred.resolve(ServiceErrorUtility.HandleFriendlyNoResponseFailure());
        // });
            deferred.resolve({Success:true});
        
        return deferred.promise;
    };
});