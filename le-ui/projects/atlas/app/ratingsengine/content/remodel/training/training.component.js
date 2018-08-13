angular.module('lp.ratingsengine.remodel.training', [])
.config(function($stateProvider) {
    $stateProvider
        .state('home.ratingsengine.remodel.training', {
            url: '/training',
            resolve: {
                ratingEngine: ['RatingsEngineStore', function (RatingsEngineStore) {
                    return RatingsEngineStore.getRatingEngine();
                }],
                segments: ['SegmentService', function (SegmentService) {
                    return SegmentService.GetSegments();
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
                'wizard_content@home.ratingsengine.remodel': 'ratingsEngineAITraining'
            }
        });
});