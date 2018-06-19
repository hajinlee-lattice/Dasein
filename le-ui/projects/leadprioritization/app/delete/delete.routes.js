angular
.module('lp.delete', [
    'lp.delete.entry'
])
.config(function($stateProvider) {
    $stateProvider
        .state('home.delete', {
            url: '/delete',
            onEnter: ['$state', 'BrowserStorageUtility', function($state, BrowserStorageUtility) {
                var ClientSession = BrowserStorageUtility.getClientSession();
                var hasAccessRights = ClientSession.AccessLevel != 'EXTERNAL_USER';
                if (!hasAccessRights) {
                    $state.go('home');
                }
            }],
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
                    template: '<delete-entry></delete-entry>'
                }
            }
        });
});