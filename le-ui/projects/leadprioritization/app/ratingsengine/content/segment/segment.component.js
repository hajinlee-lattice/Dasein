angular.module('lp.ratingsengine.wizard.segment', [])
.controller('RatingsEngineSegment', function(
    $scope, $state, $stateParams, ResourceUtility, RatingsEngineStore, Segments, QueryStore
) {
    var vm = this,
    	segmentIds = [];

    angular.extend(vm, {
        savedSegment: RatingsEngineStore.getSegment() || null,
        // SegmentsUtility: SegmentsUtility,
        segments: Segments,
        stateParams: $stateParams,
        currentPage: 1,
        pageSize: 15
    });

    $scope.$watch('vm.search', function(newValue, oldValue) {
        if(vm.search || oldValue) {
            vm.currentPage = 1;
        }
    });

    vm.init = function() {

    	console.log(vm.savedSegment);

    	vm.filteredSegments = vm.segments.slice(0, 15);
    	
    	angular.forEach(vm.filteredSegments, function(segment) {
            var segmentId = segment.name;
            segmentIds.push(segmentId);
        });

        RatingsEngineStore.getSegmentsCounts(segmentIds).then(function(response){
            console.log(response);
        });

    }

    vm.setSegment = function(segmentId) {
    	RatingsEngineStore.setSegment(segmentId);
    }


    vm.init();
});
