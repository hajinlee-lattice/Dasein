angular
    .module('pd.markets', [
        'pd.markets.dashboard'
    ])
    .controller('MarketsCtrl', function($scope, $rootScope, MarketsService) {
        $scope.marketNameToAttributes = {};

        MarketsService.getAllTargetMarkets().then(function(targetMarkets) {
            for (var i = 0; i < targetMarkets; i++) {
                var marketAttributes = {};
                var targetMarket = targetMarkets[i];
                var targetMarketName = targetMarket.name;
                
                marketAttributes["created"] = targetMarket.creation_timestamp;
                // TODO: get the other attributes page needs
                $scope.marketNameToAttributes[targetMarketName] = marketAttributes;
            }
        });
    }
);