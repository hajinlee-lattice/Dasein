angular.module('lp.ratingsengine.ratingsenginetabs', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.controller('RatingsEngineTabsController', function ($filter, RatingsEngineStore, ResourceUtility) {
    var vm = this;

    angular.extend(vm, {
        ResourceUtility: ResourceUtility,
        current: RatingsEngineStore.current
    });

    vm.count = function(type) {
        // true makes this $filter match strict strings, instead of contains
        return $filter('filter')(vm.current.ratings, { status: type }, true).length;
    }
});