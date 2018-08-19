angular
.module('lp.models.ratings')
.service('ModelRatingsService', function($http, $q, $state) {
    
    this.MostRecentConfiguration = function(id) {
        var deferred = $q.defer(),
            result,
            id = id || '',
            url = '/pls/bucketedscore/abcdbuckets/uptodate/' + id;

        $http({
            method: 'GET',
            url: url,
            headers: {
                'Accept': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                var result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );

        return deferred.promise;
    }

    this.MostRecentConfigurationRatingEngine = function(id) {
        var deferred = $q.defer(),
            result,
            id = id || '',
            url = '/pls/bucketedscore/abcdbuckets/uptodate/ratingengine/' + id;

        $http({
            method: 'GET',
            url: url,
            headers: {
                'Accept': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                var result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );

        return deferred.promise;
    }

    this.ScoringHistory = function(engineId) {
        var deferred = $q.defer(),
            engineId = engineId || '',
            url = '/pls/ratingengines/' + engineId + '/publishedhistory';

        $http({
            method: 'GET',
            url: url,
            headers: {
                'Accept': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                var result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );

        return deferred.promise;
    }

    this.HistoricalABCDBuckets = function(id) {
        var deferred = $q.defer(),
            result,
            id = id || '',
            url = '/pls/bucketedscore/abcdbuckets/' + id;

        $http({
            method: 'GET',
            url: url,
            headers: {
                'Accept': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                var result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );

        return deferred.promise;
    }

    this.HistoricalABCDBucketsRatingEngine = function(id) {
        var deferred = $q.defer(),
            result,
            id = id || '',
            url = '/pls/bucketedscore/abcdbuckets/ratingengine/' + id;

        $http({
            method: 'GET',
            url: url,
            headers: {
                'Accept': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                var result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );

        return deferred.promise;
    }

    this.GetBucketedScoresSummary = function(id) {
        var deferred = $q.defer();
        var result;
        var id = id || '';
        var url = '/pls/bucketedscore/summary/' + id;

        $http({
            method: 'GET',
            url: url,
            headers: {
                'Accept': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                var result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );

        return deferred.promise;
    }

    this.GetBucketedScoresSummaryRatingEngine = function(ratingId, modelId) {
        var deferred = $q.defer();
        var result;
        var id = id || '';
        var url = '/pls/bucketedscore/summary/ratingengine/' + ratingId + '/model/' + modelId;

        $http({
            method: 'GET',
            url: url,
            headers: {
                'Accept': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                var result = response.data;
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );

        return deferred.promise;
    }

    this.CreateABCDBuckets = function(id, buckets) {
        var deferred = $q.defer();

        $http({
            method: 'POST',
            url: '/pls/bucketedscore/abcdbuckets/' + id,
            data: buckets,
            headers: {
                'Content-Type': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                var result = {
                        data: response.data,
                        success: true
                    }
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );

        return deferred.promise;
    }
    this.CreateABCDBucketsRatingEngine = function(ratingId, modelId, buckets) {
        var deferred = $q.defer();
        var url = '/pls/ratingengines/'+ratingId+'/ratingmodels/'+modelId+'/setScoringIteration';
        $http({
            method: 'POST',
            url: url,
            data: buckets,
            headers: {
                'Content-Type': 'application/json'
            }
        }).then(
            function onSuccess(response) {
                var result = {
                        data: response.data,
                        success: true
                    }
                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorMsg = response.data.errorMsg || 'unspecified error';
                deferred.resolve(errorMsg);
            }
        );

        return deferred.promise;
    }
    // this.CreateABCDBucketsRatingEngine = function(ratingId, modelId, buckets) {
    //     var deferred = $q.defer();

    //     $http({
    //         method: 'POST',
    //         url: '/pls/bucketedscore/abcdbuckets/ratingengine/' + ratingId + '/model/' + modelId,
    //         data: buckets,
    //         headers: {
    //             'Content-Type': 'application/json'
    //         }
    //     }).then(
    //         function onSuccess(response) {
    //             var result = {
    //                 data: response.data,
    //                 success: true
    //             };
                
    //             deferred.resolve(result);

    //         }, function onError(response) {
    //             if (!response.data) {
    //                 response.data = {};
    //             }

    //             var errorMsg = response.data.errorMsg || 'unspecified error';
    //             deferred.resolve(errorMsg);
    //         }
    //     );

    //     return deferred.promise;
    // }
});
