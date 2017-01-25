angular
.module('lp.models.ratings')
.service('ModelRatingsService', function($http, $q, $state) {
    
    this.GetUpToDateABCDBuckets = function(id) {
        // Need more clarification on what this one is doing
    }

    this.GetABCDBuckets = function(id) {
        var deferred = $q.defer();
        var result;
        var id = id || '';
        var url = '/pls/bucketedscores/abcdbuckets/mocked/' + id;

        $http({
            method: 'GET',
            url: url,
            headers: {
                'Accept': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                result = response.data;
                deferred.resolve(result);

            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.reject(errorMsg);
            }
        );

        return deferred.promise;
    }


    this.GetBucketedScoresSummary = function(id) {
        var deferred = $q.defer();
        var result;
        var id = id || '';
        var url = '/pls/bucketedscores/summary/mocked/' + id;

        $http({
            method: 'GET',
            url: url,
            headers: {
                'Accept': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                result = response.data;
                deferred.resolve(result);

            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.reject(errorMsg);
            }
        );

        return deferred.promise;
    }


    this.CreateABCDBuckets = function(id) {
        var deferred = $q.defer(),
            data = [{

            }];

        $http({
            method: 'POST',
            url: '/pls/bucketedscore/abcdbuckets/' + id,
            data: data,
            headers: { 'Content-Type': 'application/json' }
        }).then(
            function onSuccess(response) {
                var result = {
                    data: response.data,
                    success: true
                };
                
                console.log(result);

                deferred.resolve(result);

            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                console.log(errorMsg);
                deferred.resolve(errorMsg);
            }
        );

        return deferred.promise;
    }

});
