angular
.module('lp.marketo', [])
.service('MarketoService', function($http, $q) {
    this.GetMarketoCredentials = function() {
        var deferred = $q.defer();
        var result;
        var url = '/pls/marketocredentials/';

        $http({
            method: 'GET',
            url: url,
            headers: {
                'Accept': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                result = {
                    success: true,
                    resultObj: response.data
                };
                deferred.resolve(result);
            }, function onError(response) {

            }
        );

        return deferred.promise;
    }
});
