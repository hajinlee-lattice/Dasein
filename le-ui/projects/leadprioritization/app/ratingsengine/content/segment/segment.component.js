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
        showPagination: true,
        hasSegments: true,
        sortBy: 'Selected',
        segmentsKeyMap: {}
    });

    var makeSegmentsKeyMap = function(segments) {
        var segmentsKeyMap = {};
        segments.forEach(function(segment, index) {
            segmentsKeyMap[segment.name] = index;
        });
        return segmentsKeyMap;
    }

    $scope.$watch('vm.search', function(newValue, oldValue) {
        if(vm.search || oldValue) {
            vm.currentPage = 1;
        }
    });

    vm.init = function() {
    	vm.filteredSegments = vm.segments.slice(0, 10);
        vm.segmentsKeyMap = makeSegmentsKeyMap(vm.segments);
    	vm.getCounts(vm.segments);
        if(vm.segments.length === 0){
            vm.hasSegments = false;
            vm.isValid = false;
        }

        if(vm.filteredSegments.length < 10){
            vm.showPagination = false;
        }

        RatingsEngineStore.setValidation('segment', false);
        if($stateParams.rating_id) {
            RatingsEngineStore.getRating($stateParams.rating_id).then(function(rating){
                vm.stored.segment_selection = rating.segment.name;
                vm.setSegment(rating.segment);
                vm.block_user = false;
                RatingsEngineStore.setValidation('segment', true);

                makeItemFirst = function (name){
                    for (var i = 0; i < vm.segments.length; i++){
                        if (vm.segments[i].name == name){
                            vm.segments[i].Selected = true;
                        }
                    }
                }
                makeItemFirst(vm.stored.segment_selection);
            });
        } else {
            vm.block_user = false;
        }
    }

    vm.endsWith = function(item, string) {
        var reg = new RegExp(string + '$'),
            item = item || '',
            match = item.match(reg);
        if(match) {
            return true;
        }
        return false;
    }

    function chunk (arr, len) {
        var chunks = [],
        i = 0,
        n = arr.length;

        while (i < n) {
            chunks.push(arr.slice(i, i += len));
        }

        return chunks;
    }

    vm.getCounts = function(segments) {
    	var segmentIds = [],
            _segments = {};

    	angular.forEach(segments, function(segment) {
            var segmentId = segment.name;
            segmentIds.push(segmentId);
        });

        var chunk_size = Math.round(segmentIds.length / 6),
            segmentChunks = chunk(segmentIds, chunk_size) || [];

        angular.forEach(segmentChunks, function(ids, index) {
            RatingsEngineStore.getSegmentsCounts(ids).then(function(response){
                ids.forEach(function(id) {
                    if(vm.segmentsKeyMap && vm.segmentsKeyMap[id] && response.segmentIdCoverageMap && response.segmentIdCoverageMap[id]) {
                        vm.segments[vm.segmentsKeyMap[id]].numAccounts = (response.segmentIdCoverageMap[id].accountCount ? response.segmentIdCoverageMap[id].accountCount : 0);
                        vm.segments[vm.segmentsKeyMap[id]].numContacts = (response.segmentIdCoverageMap[id].contactCount ? response.segmentIdCoverageMap[id].contactCount : 0);
                    } else {
                        vm.segments[vm.segmentsKeyMap[id]].numAccounts = 0;
                        vm.segments[vm.segmentsKeyMap[id]].numContacts = 0;
                    }
                });
                var done = (index+1 === segmentChunks.length);
                if(done) {
                    vm.loadingSupplementaryData = false;
                }
            })
        })
    }

    vm.setSegment = function(segment) {
        RatingsEngineStore.setValidation('segment', true);
    	RatingsEngineStore.setSegment(segment);
    }


    vm.init();
});
