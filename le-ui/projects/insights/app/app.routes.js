angular.module('insightsApp')
.run(function($rootScope, $state, ResourceUtility, ServiceErrorUtility) {
    $rootScope.$on('$stateChangeStart', function(evt, toState, params, fromState, fromParams) {
        var LoadingString = ResourceUtility.getString("");

        if (toState.redirectTo) {
            evt.preventDefault();
            $state.go(toState.redirectTo, params);
        }

        ShowSpinner(LoadingString);
        ServiceErrorUtility.hideBanner();
    });

    $rootScope.$on('$stateChangeError', function(event, toState, toParams, fromState, fromParams, error){ 
        console.log('-!- error changing state:', error);

        event.preventDefault();
    });
})
.config(function($stateProvider, $urlRouterProvider, $locationProvider) {
    $locationProvider.html5Mode(true);
    $urlRouterProvider.otherwise('/tenant/');

    $stateProvider
        .state('home', {
            url: '/tenant/:tenantName',
            resolve: {
                ClientSession: function(BrowserStorageUtility) {
                    return BrowserStorageUtility.getClientSession();
                },
                Tenant: function(ClientSession) {
                    return ClientSession.Tenant;
                },
                FeatureFlags: function($q, FeatureFlagService) {
                    var deferred = $q.defer();
                    
                    FeatureFlagService.GetAllFlags().then(function() {
                        deferred.resolve();
                    });
                    
                    return deferred.promise;
                },
                ResourceStrings: function($q, ResourceStringsService, ClientSession) {
                    var deferred = $q.defer();

                    ResourceStringsService.GetInternalResourceStringsForLocale(ClientSession.Locale).then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                }
            },
            views: {
                "main": {
                    controller: function($rootScope, $stateParams, $state, Tenant) {
                        var tenantName = $stateParams.tenantName;

                        console.log('hello world', $stateParams, tenantName);
                        if (tenantName != Tenant.DisplayName) {
                            $rootScope.tenantName = window.escape(Tenant.DisplayName);
                            $rootScope.tenantId = window.escape(Tenant.Identifier);
                            
                            $state.go('home.main', { 
                                tenantName: Tenant.DisplayName
                            });
                        }
                    }
                }
            }
        })
        .state('home.main', {
            url: '/main',
            redirectTo: 'home.datacloud.explorer.browse'
        });
});

function ShowSpinner(LoadingString, type) {
    // state change spinner
    var element = $('#mainContentView'),
        LoadingString = LoadingString || '',
        type = type || 'lattice';
        
    // jump to top of page during state change
    angular.element(window).scrollTop(0,0);

    element
        .children()
            .addClass('inactive-disabled');
    
    element
        .css({
            position:'relative'
        })
        .prepend(
            $(
                '<section class="loading-spinner ' + type + '">' +
                '<h2 class="text-center">' + LoadingString + '</h2>' +
                '<div class="meter"><span class="indeterminate"></span></div>' +
                '</section>'
            )
        );

    setTimeout(function() {
        $('section.loading-spinner').addClass('show-spinner');
    }, 1);
}