angular.module('insightsApp')
.run(function($transitions) {
    $transitions.onStart({}, function(trans) {
        ShowSpinner(LoadingString);
        trans.injector().get('ServiceErrorUtility').hideBanner();
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
                    controller: function($state, AuthStore, LookupStore, FeatureFlagService, ApiHost) {
                        parent.postMessage("init", '*');

                        console.log('### insightsApp: initialized, waiting for postMessage()')
                        window.addEventListener("message", function (event){
                            console.log('### insightsApp: received postMessage() event',event)
                            var data = ((typeof event.data).toLowerCase() == 'string')
                                ? JSON.parse(event.data)
                                : event.data;

                            var timestamp = new Date().getTime();

                            LookupStore.add('timestamp', timestamp);
                            LookupStore.add('request', data.request);
                            AuthStore.set('Bearer ' + data.Authentication);
                            
                            console.log('### insightsApp: redirectTo home.datacloud.insights')
                            
                            FeatureFlagService.GetAllFlags(ApiHost).then(function(flags) {
                                $state.go('home.datacloud.insights');
                            });
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