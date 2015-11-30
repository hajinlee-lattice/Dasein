angular.module('mainApp.markets.controllers.MarketsController', [
    'mainApp.markets.services.MarketsService'
])
.controller('MarketsCtrl', function($scope, $rootScope, MarketsService) {
    $scope.targetMarkets;

    MarketsService.getAllTargetMarkets().then(function(targetMarkets) {
        $scope.targetMarkets = targetMarkets;
        console.log(targetMarkets);
    });
});