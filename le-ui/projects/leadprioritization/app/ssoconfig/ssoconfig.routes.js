angular
.module('lp.ssoconfig', [
    'lp.ssoconfig.configure'
])
.config(function($stateProvider) {
    $stateProvider
        .state('home.ssoconfig', {
            url: '/ssoconfig',
            // onEnter: ['AuthorizationUtility', function(AuthorizationUtility) {
            //     AuthorizationUtility.redirectIfNotAuthorized(AuthorizationUtility.adminsOnly, {}, 'home');
            // }],
            params: {
                pageIcon: 'ico-cog',
                pageTitle: 'SSO Configuration'
            },
            resolve: {
                SSOConfiguration: function($q, SSOConfigStore) {
                    var deferred = $q.defer();

                    SSOConfigStore.getSAMLConfig().then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                },
                ServiceProviderURLs: function($q, SSOConfigStore) {
                    var deferred = $q.defer();

                    SSOConfigStore.getURIInfo().then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }
            },
            views: {
                'main@': 'ssoConfig'
            }
        });
});