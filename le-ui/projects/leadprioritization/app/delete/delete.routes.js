angular
.module('lp.delete', [
    'lp.delete.entry'
])
.config(function($stateProvider) {
    $stateProvider
        .state('home.delete', {
            url: '/delete',
            resolve: {
                EntitiesCount: function($q, QueryStore) {
                    var deferred = $q.defer();

                    QueryStore.getEntitiesCounts().then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;

                } 
            },
            views: {
                'main@': {
                    // controller: 'DeleteEntry',
                    // controllerAs: 'vm',
                    // templateUrl: 'app/delete/delete.component.html'
                    template: '<delete-entry> </delete-entry>'
                }
            }
        });
});