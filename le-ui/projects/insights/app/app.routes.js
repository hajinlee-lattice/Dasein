angular.module('insightsApp')
.run(function($rootScope, $state) {
    console.log('Hello world');
    $rootScope.$on('$stateChangeError', function(event, toState, toParams, fromState, fromParams, error){ 
        console.log('-!- error changing state:', error);

        event.preventDefault();
    });
})
.config(function($stateProvider, $urlRouterProvider, $locationProvider) {
    //$locationProvider.html5Mode(true);
});