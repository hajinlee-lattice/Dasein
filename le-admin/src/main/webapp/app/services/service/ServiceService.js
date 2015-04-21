var app = angular.module("app.services.service.ServiceService", [
    'le.common.util.UnderscoreUtility',
    'app.core.util.SessionUtility'
]);

app.service('ServiceService', function($q, $http, _, SessionUtility){

    this.GetRegisteredServices = function() {
        var defer = $q.defer();

        var result = {
            success: true,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'GET',
            url: '/admin/services'
        }).success(function(data) {
            result.resultObj = data;
            defer.resolve(result);
        }).error(function(err, status){
            SessionUtility.handleAJAXError(err, status);
        });

        return defer.promise;
    };

});
