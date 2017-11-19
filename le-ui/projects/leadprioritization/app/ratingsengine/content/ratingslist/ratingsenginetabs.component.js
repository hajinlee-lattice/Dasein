angular.module('lp.ratingsengine.ratingsenginetabs', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.controller('RatingsEngineTabsController', function ($filter, ResourceUtility, RatingsEngineStore) {
    var vm = this;

    angular.extend(vm, {
        ResourceUtility: ResourceUtility,
        current: RatingsEngineStore.current
    });

    vm.count = function(type) {
        return $filter('filter')(vm.current.ratings, { status: type }, true).length;
    }
});