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
        pageSize: 10,
        block_user: true,
        loadingSupplementaryData: true,
        showPagination: true
    });

    $scope.$watch('vm.search', function(newValue, oldValue) {
        if(vm.search || oldValue) {
            vm.currentPage = 1;
        }
    });

    $scope.$watch('vm.currentPage', function(newValue, oldValue) {
        if(vm.currentPage != oldValue) {
        	vm.filteredSegments = vm.segments.slice((10 * (vm.currentPage - 1)), (10 * vm.currentPage));
        	
        	console.log(vm.filteredSegments);

        	if(!vm.filteredSegments[0].numAccounts) {
	        	vm.loadingSupplementaryData = true;
	        	vm.getCounts(vm.filteredSegments);
	        }
        }
    });

    vm.init = function() {

    	vm.filteredSegments = vm.segments.slice(0, 10);
    	vm.getCounts(vm.filteredSegments);

        if(vm.filteredSegments.length < 10){
            vm.showPagination = false;
        }

        if($stateParams.rating_id) {
            RatingsEngineStore.getRating($stateParams.rating_id).then(function(rating){
                vm.stored.segment_selection = rating.segment.name;
                vm.setSegment(rating.segment);
                vm.block_user = false;

                // if(vm.stored.segment_selection){
                //     console.log("has stored");
                //     vm.initSegments = vm.segments;
                // } else {
                //     console.log("not stored");
                //     vm.initSegments = vm.segments;
                // }


                // There's probably a better way to do this with orderBy and a filter
                // but this gets the selected segment and move it to the first position in the array
                var move = function (array, fromIndex, toIndex) {
                    array.splice(toIndex, 0, array.splice(fromIndex, 1)[0] );
                    return array;
                }
                makeItemFirst = function (name){
                    for (var i = 0; i < vm.segments.length; i++){
                        if (vm.segments[i].name == name){
                            vm.segments = move(vm.segments, i, 0);
                        }
                    }
                }
                makeItemFirst(vm.stored.segment_selection);



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
