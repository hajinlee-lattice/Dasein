angular.module('loginApp')
.run(function($rootScope, $state) {
    $rootScope.$on('$stateChangeError', function(event, toState, toParams, fromState, fromParams, error){ 
        console.log('-!- error changing state:', event, toState, toParams, fromState, fromParams, error);

        event.preventDefault();
        
        if ($state.current.name != toState.name) {
            $state.go('login.form');
        }
    });
})
.config(function($stateProvider, $urlRouterProvider, $locationProvider) {
    $locationProvider.html5Mode(true);
    $urlRouterProvider.otherwise('/');

    $stateProvider
        .state('logout', {
            url: 'logout',
            views: {
                "main": {
                    controller: function(LoginService) {
                        LoginService.Logout();
                    },
                    template: ''
                }
            }
        });
});