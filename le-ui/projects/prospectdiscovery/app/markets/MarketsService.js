angular
    .module('pd.markets')
    .service('MarketsService', function($http, $q) {
        this.getAllTargetMarkets = function() {
            var deferred = $q.defer();
            
            $http({
                method: 'GET',
                url: '/pls/targetmarkets',
            }).then(
                function onSuccess(response) {
                    var result = response.data;
                    return deferred.resolve(result);
                }, function onError(response) {
                        
                }
            );

            return deferred.promise;
        };
    }
);