angular.module('lp.ratingsengine.ratingsenginetabs', [
    'mainApp.appCommon.utilities.ResourceUtility'
    ])
.controller('RatingsEngineTabsController', function (
    $state, $stateParams, $timeout, $scope, BrowserStorageUtility,
    ResourceUtility, RatingList
) {
    var vm = this;

    angular.extend(vm, {
        stateParams: $stateParams,
        ratings: RatingList || []
    });

    vm.init = function() {
        vm.activeCount = function() {
            var count = 0;
            angular.forEach(vm.ratings, function(rating){
                count += rating.status.ACTIVE ? 1 : 0;
            });
            return count; 
        };
    }
    vm.init();

});