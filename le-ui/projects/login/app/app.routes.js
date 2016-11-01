angular.module('loginApp')
.run(function($rootScope, $state) {
    $rootScope.$on('$stateChangeError', function(event, toState, toParams, fromState, fromParams, error){ 
        console.log('-!- error changing state:', error);

        event.preventDefault();

        $state.go('login.form');
    });
})
.config(function($stateProvider, $urlRouterProvider, $locationProvider) {
    //$locationProvider.html5Mode(true);
    $urlRouterProvider.otherwise('/form');

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