angular
    .module('mainApp')

    // add ability to redirect with redirectTo
    .run(['$rootScope', '$state', function($rootScope, $state) {
        $rootScope.$on('$stateChangeStart', function(evt, to, params) {
          if (to.redirectTo) {
            evt.preventDefault();
            $state.go(to.redirectTo, params)
          }
        });
    }])

    // define routes for PD application.
    .config(['$stateProvider', '$urlRouterProvider', function($stateProvider, $urlRouterProvider) {
        $urlRouterProvider.otherwise('/');

        $stateProvider
            .state('home', {
                url: '/',
                redirectTo: 'markets'
            });
    }]);