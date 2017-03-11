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
        console.log('-!- error changing state:', error, event, toState, toParams, fromState, fromParams);

        event.preventDefault();
    });
})
.config(function($stateProvider, $urlRouterProvider, $locationProvider) {
    $locationProvider.html5Mode(true);
    $urlRouterProvider.otherwise('/');

    $stateProvider
        .state('home', {
            url: '/',
            resolve: {
                ResourceStrings: function($q, ResourceStringsService) {
                    var deferred = $q.defer();

                    ResourceStringsService.GetInternalResourceStringsForLocale('en-US').then(function(result) {
                        deferred.resolve(result);
                    });

                    return deferred.promise;
                },
                ApiHost: function() {
                    return '/ulysses';
                }
            },
            views: {
                "main": {
                    controller: function($state, AuthStore, LookupStore) {
                        parent.postMessage("init", '*');

                        window.addEventListener("message", function (event){
                            var data = ((typeof event.data).toLowerCase() == 'string')
                                ? JSON.parse(event.data)
                                : event.data;

                            var timestamp = new Date().getTime();

                            LookupStore.add('timestamp', timestamp);
                            LookupStore.add('request', data.request);
                            AuthStore.set('Bearer ' + data.Authentication);
                            $state.go('home.datacloud.insights');
                        }, false);
                    }
                }
            }
        })
        .state('home.error', {
            url: '/error',
            views: {
                "main": {
                    template: ''
                }
            }
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