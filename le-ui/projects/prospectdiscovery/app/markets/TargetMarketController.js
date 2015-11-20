angular.module('mainApp.targetMarkets.controllers.TargetMarketController', [
    'mainApp.targetMarkets.services.TargetMarketService'
])
.controller('TargetMarketController', function($scope, $rootScope, TargetMarketService) {
    $scope.targetMarkets;

    TargetMarketService.getAllTargetMarkets().then(function(targetMarkets) {
        $scope.targetMarkets = targetMarkets;
        console.log(targetMarkets);
    });
});