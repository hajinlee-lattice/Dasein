angular.module('lp.ratingsengine.ratingsenginetabs', [
    'mainApp.appCommon.utilities.ResourceUtility'
    ])
.controller('RatingsEngineTabsController', function (
    $state, $stateParams, $timeout, $scope, $filter,
    BrowserStorageUtility, ResourceUtility, RatingList
) {
    var vm = this;

    angular.extend(vm, {
        stateParams: $stateParams,
        ratings: RatingList || []
    });

    vm.init = function() {

        // console.log(vm.ratings);

        vm.inactiveCount = $filter('filter')(vm.ratings, { status: 'INACTIVE' }).length;
        vm.activeCount = vm.ratings.length - vm.inactiveCount; 

    }
    vm.init();

});