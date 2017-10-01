angular.module('lp.ratingsengine.wizard.segment', [])
.controller('RatingsEngineSegment', function(
    $scope, $state, $stateParams, ResourceUtility, RatingsEngineStore, DataCloudStore, Segments, QueryStore, CurrentRatingEngine
) {
    var vm = this;

    angular.extend(vm, {
        currentRating: CurrentRatingEngine,
        stored: RatingsEngineStore.segment_form,
        segments: Segments,
        stateParams: $stateParams,
        currentPage: 1,
        pageSize: 15,
        block_user: true,
        loadingSupplementaryData: true
    });

    $scope.$watch('vm.search', function(newValue, oldValue) {
        if(vm.search || oldValue) {
            vm.currentPage = 1;
        }
    });

    $scope.$watch('vm.currentPage', function(newValue, oldValue) {
        if(vm.currentPage != oldValue) {
        	vm.filteredSegments = vm.segments.slice((15 * (vm.currentPage - 1)), (15 * vm.currentPage));
        	
        	console.log(vm.filteredSegments);

        	if(!vm.filteredSegments[0].numAccounts) {
	        	vm.loadingSupplementaryData = true;
	        	vm.getCounts(vm.filteredSegments);
	        }
        }
    });

    vm.init = function() {

    	vm.filteredSegments = vm.segments.slice(0, 15);
    	vm.getCounts(vm.filteredSegments);

        if($stateParams.rating_id) {
            RatingsEngineStore.getRating($stateParams.rating_id).then(function(rating){
                vm.stored.segment_selection = rating.segment.name;
                vm.setSegment(rating.segment);
                vm.block_user = false;
            });
        } else {
            vm.block_user = false;
        }

    }

    vm.getCounts = function(filteredSegments) {

    	segmentIds = [];

    	angular.forEach(filteredSegments, function(segment) {
            var segmentId = segment.name;
            segmentIds.push(segmentId);
        });
        RatingsEngineStore.getSegmentsCounts(segmentIds).then(function(response){
            angular.forEach(filteredSegments, function(segment) {
            	segment.numAccounts = response.segmentIdCoverageMap[segment.name].accountCount;
            	segment.numContacts = response.segmentIdCoverageMap[segment.name].contactCount;
            });
            vm.loadingSupplementaryData = false;
        });
    }

    vm.setSegment = function(segment) {
        console.log("set segment", segment);
    	RatingsEngineStore.setSegment(segment);
    }


    vm.init();
});
