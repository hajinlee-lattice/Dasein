angular.module('lp.ratingsengine.wizard.segment', [])
.controller('RatingsEngineSegment', function(
    $scope, $state, $stateParams, ResourceUtility, RatingsEngineStore, DataCloudStore, Segments, QueryStore, CurrentRatingEngine, FeatureFlagService
) {
    var vm = this,
        flags = FeatureFlagService.Flags();

    angular.extend(vm, {
        currentRating: CurrentRatingEngine,
        stored: RatingsEngineStore.segment_form,
        segments: Segments,
        stateParams: $stateParams,
        currentPage: 1,
        pageSize: 10,
        block_user: true,
        showPagination: true,
        hasSegments: true,
        sortBy: 'Selected',
        segmentsKeyMap: {},
        showScoreTrainingFileOption: FeatureFlagService.FlagIsEnabled(flags.SCORE_EXTERNAL_FILE),
        scoreTrainingFile: false
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
                if (rating.segment) {
                    vm.stored.segment_selection = rating.segment.name;
                    vm.setSegment(rating.segment);
                } else if (rating.type == 'CUSTOM_EVENT') { // LPI-version of custom-event modeling
                    vm.scoreTrainingFile = true;
                }
                vm.block_user = false;
                RatingsEngineStore.setValidation('segment', true);

                var makeItemFirst = function (name){
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

        RatingsEngineStore.setModelingStrategy(vm.stateParams.engineType);
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

    function chunk (arr, n) {
        if (n < 2)
            return [arr];

        var len = arr.length,
            out = [],
            i = 0,
            size;

        if (len % n === 0) {
            size = Math.floor(len / n);
            while (i < len) {
                out.push(arr.slice(i, i += size));
            }
        } else {
            while (i < len) {
                size = Math.ceil((len - i) / n--);
                out.push(arr.slice(i, i += size));
            }
        }

        return out;
    }

    vm.getCounts = function(segments) {
    	var segmentIds = [],
            _segments = {};

    	angular.forEach(segments, function(segment) {
            var segmentId = segment.name;
            segmentIds.push(segmentId);
        });

        var segmentChunks = chunk(segmentIds, 5) || [];

        angular.forEach(segmentChunks, function(ids, index) {
            RatingsEngineStore.getSegmentsCounts(ids).then(function(response){
                ids.forEach(function(id) {
                    if(vm.segmentsKeyMap && vm.segmentsKeyMap[id] !== 'undefined' && response.segmentIdCoverageMap && response.segmentIdCoverageMap[id]) {
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
            });
        });
    }

    vm.setSegment = function(segment) {

        // console.log(segment);

        RatingsEngineStore.setValidation('segment', true);
    	RatingsEngineStore.setSegment(segment);
        RatingsEngineStore.setCustomEventModelingType('CDL');
    }

    vm.selectTrainingFile = function() {
        if (vm.scoreTrainingFile) {
            // RatingsEngineStore.setSegment(vm.segments[vm.segments.length - 1]);
            RatingsEngineStore.setSegment(null);

            vm.stored.segment_selection = "";
            RatingsEngineStore.setCustomEventModelingType('LPI');
            RatingsEngineStore.setValidation('segment', true);
        } else {
            RatingsEngineStore.setValidation('segment', false);
        }
    }


    vm.init();
});
