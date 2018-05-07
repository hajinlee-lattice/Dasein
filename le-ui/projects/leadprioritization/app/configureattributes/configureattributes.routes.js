angular
.module('lp.configureattributes', [
   'lp.configureattributes.configure' 
])
.config(function($stateProvider) {
    $stateProvider
        .state('home.configureattributes', {
            url: '/configureattributes',
            onExit: function(ConfigureAttributesStore) {
               ConfigureAttributesStore.clear();
            },
            resolve: {
                PurchaseHistory: function($q, ConfigureAttributesStore) {
                    var deferred = $q.defer();

                    ConfigureAttributesStore.getPurchaseHistory().then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }
            },
            redirectTo: 'home.configureattributes.spend_change'
        })
        .state('home.configureattributes.spend_change', {
            url: '/spend_change',
            views: {
                'main@': {
                    template: '<configure-attributes-configure></configure-attributes-configure>'
                }
            },
        })
        .state('home.configureattributes.spend_over_time', {
            url: '/spend_over_time',
            views: {
                'main@': {
                    template: '<configure-attributes-configure></configure-attributes-configure>'
                }
            },
        })
        .state('home.configureattributes.share_of_wallet', {
            url: '/share_of_wallet',
            views: {
                'main@': {
                    template: '<configure-attributes-configure></configure-attributes-configure>'
                }
            },
        })
        .state('home.configureattributes.margin', {
            url: '/margin',
            views: {
                'main@': {
                    template: '<configure-attributes-configure></configure-attributes-configure>'
                }
            },
        })
        .state('home.configureattributes.done', {
            url: '/done',
            views: {
                'main@': {
                    template: '<configure-attributes-configure></configure-attributes-configure>'
                }
            },
        });
});