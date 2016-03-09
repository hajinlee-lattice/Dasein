angular
    .module('pd.fingerprints', [
    ])
    .config(['$stateProvider', function($stateProvider, $urlRouterProvider) {
        $stateProvider
            .state('fingerprints', {
                url: '/fingerprints',
                views: {
                    "main": {
                        templateUrl: 'app/fingerprints/FingerprintsView.html'
                    }
                }
            });
    }])
    .controller('FingerprintsCtrl', function($scope, $rootScope) {
        
    });