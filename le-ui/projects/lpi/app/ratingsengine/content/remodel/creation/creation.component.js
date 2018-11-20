angular.module('lp.ratingsengine.remodel.creation', [])
.config(function($stateProvider) {
    $stateProvider
        .state('home.ratingsengine.remodel.training.attributes.creation', {
            url: '/creation',
            resolve: {
                ratingEngine: ['$q', '$stateParams', 'RatingsEngineStore', function ($q, $stateParams, RatingsEngineStore) {
                    var deferred = $q.defer();

                    console.log($stateParams);

                    RatingsEngineStore.getRating($stateParams.engineId).then(function (result) {
                        deferred.resolve(result)
                    });

                    return deferred.promise;
                }],
                products: ['$q', '$stateParams', 'RatingsEngineStore', function ($q, $stateParams, RatingsEngineStore) {
                    var deferred = $q.defer();

                    var params = {
                        max: 1000,
                        offset: 0
                    };
                    RatingsEngineStore.getProducts(params).then(function (result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }]
            },
            views: {
                'wizard_content@home.ratingsengine.remodel': 'ratingsEngineCreation'
            }
        });
});