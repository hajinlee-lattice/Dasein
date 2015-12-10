angular
    .module('pd.markets', [
        'pd.markets.dashboard'
    ])
    .controller('MarketsCtrl', function($scope, $rootScope, MarketsService) {
        $scope.targetMarkets;

        MarketsService.getAllTargetMarkets().then(function(targetMarkets) {
            $scope.targetMarkets = targetMarkets;
            console.log(targetMarkets);
        });
    }
);