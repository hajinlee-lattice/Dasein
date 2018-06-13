angular
.module('common.datacloud.valuepicker')
.config(function($stateProvider) {
    $stateProvider
        .state('home.segment.explorer.enumpicker', {
            url: '/picker/:entity/:fieldname',
            resolve: {
                PickerBuckets: ['$q', '$stateParams', 'QueryTreeService', 'DataCloudStore', function($q, $stateParams, QueryTreeService, DataCloudStore){
                    var deferred = $q.defer();
                    var entity = $stateParams.entity;
                    var fieldname = $stateParams.fieldname;

                    QueryTreeService.getPickerCubeData(entity, fieldname).then(function(result) {
                        deferred.resolve(result.data);
                    });
                    
                    return deferred.promise;
                }],
                Segment: function(){
                    return {};
                }
            },
            views: {
                "main@": {
                    controller: 'ValuePickerController',
                    controllerAs: 'vm',
                    templateUrl: '/components/datacloud/picker/picker.component.html'
                }
            }
        });
});
