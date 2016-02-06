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
            /*
            .state('home', {
                url: '/',
                redirectTo: 'model'
            })
            */
            .state('model', {
                url: '/model',
                redirectTo: "model.list",
                views: {
                    /*
                    "navigation@": {
                        templateUrl: './app/navigation/sidebar/BuilderView.html'
                    },
                    "summary@": {
                        templateUrl: './app/navigation/subnav/SubNavView.html'
                    }
                    */
                }
            })
            .state('model.activate', {
                url: '/model/activate',
                views: {
                    "main@": {
                        templateUrl: './app/models/views/ActivateModelView.html'
                    }
                }
            })
            .state('model.list', {
                url: '/list',
                views: {
                    "main@": {
                        templateUrl: './app/models/views/ModelListView.html'
                    }   
                }
            });
    }]);