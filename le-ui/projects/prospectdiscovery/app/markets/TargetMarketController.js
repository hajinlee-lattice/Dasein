angular.module('mainApp.markets.controllers.TargetMarketController', [
    'mainApp.markets.services.TargetMarketService'
])
.controller('TargetMarketController', function($scope, $rootScope, TargetMarketService) {
    $scope.targetMarkets;

    TargetMarketService.getAllTargetMarkets().then(function(targetMarkets) {
        $scope.targetMarkets = targetMarkets;
        console.log(targetMarkets);
    });
});