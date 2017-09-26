angular.module('lp.ratingsengine.wizard.segment', [])
.controller('RatingsEngineSegment', function(
    $scope, $state, $stateParams, ResourceUtility, RatingsEngineStore, DataCloudStore, Segments, QueryStore
) {
    var vm = this,
    	segmentIds = [];

    angular.extend(vm, {
        stored: RatingsEngineStore.segment_form,
        savedSegment: RatingsEngineStore.getSegment() || null,
        // SegmentsUtility: SegmentsUtility,
        segments: Segments,
        stateParams: $stateParams,
        currentPage: 1,
        pageSize: 15,
        block_user: true
    });

    $scope.$watch('vm.search', function(newValue, oldValue) {
        if(vm.search || oldValue) {
            vm.currentPage = 1;
        }
    });

    vm.init = function() {

    	vm.filteredSegments = vm.segments.slice(0, 15);
    	
    	angular.forEach(vm.filteredSegments, function(segment) {
            var segmentId = segment.name;
            segmentIds.push(segmentId);
        });

        if($stateParams.rating_id) {
            RatingsEngineStore.getRating($stateParams.rating_id).then(function(rating){
                vm.stored.segment_selection = rating.segment.name;
                vm.setSegment(rating.segment);
                vm.block_user = false;
            });
        } else {
            vm.block_user = false;
        }

        RatingsEngineStore.getSegmentsCounts(segmentIds).then(function(response){
            //console.log(response);
        });

    }

    vm.setSegment = function(segment) {
    	RatingsEngineStore.setSegment(segment);
    }


    vm.init();
});
