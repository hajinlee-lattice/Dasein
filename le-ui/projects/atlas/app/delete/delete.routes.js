angular
.module('lp.delete', [
    'lp.delete.entry',
    'mainApp.core.utilities.AuthorizationUtility'
])
.config(function($stateProvider) {
    $stateProvider
        .state('home.delete', {
            url: '/delete',
            onEnter: ['AuthorizationUtility', function(AuthorizationUtility) {
                AuthorizationUtility.redirectIfNotAuthorized(AuthorizationUtility.excludeExternalUser, {}, 'home');
            }],
            onExit: ['Banner', function(Banner) {
                Banner.reset();
            }],
            resolve: {
                EntitiesCount: function($q, QueryStore) {
                    var deferred = $q.defer();

                    QueryStore.getCollectionStatus().then(function(result) {
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