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
                products: ['$q', 'RatingsEngineStore', function ($q, RatingsEngineStore) {
                    var deferred = $q.defer();

                    var params = {
                        max: 1000,
                        offset: 0
                    };
                    RatingsEngineStore.getProducts(params).then(function (result) {
                        deferred.resolve(result);
                    });
                    return deferred.promise;
                }],
                iteration: ['$q', '$stateParams', 'RatingsEngineStore', 'AtlasRemodelStore', 'ratingEngine', function($q, $stateParams, RatingsEngineStore, AtlasRemodelStore, ratingEngine){
                    var deferred = $q.defer(),
                        engineId = $stateParams.engineId,
                        modelId = $stateParams.modelId;

                    console.log($stateParams);

                    RatingsEngineStore.getRatingModel(engineId, modelId).then(function(result){
                        AtlasRemodelStore.setRemodelIteration(result);
                        RatingsEngineStore.setRatingEngine(ratingEngine);

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