angular.module('lp.ratingsengine.wizard.segment', [])
.controller('RatingsEngineSegment', function(
    $scope, $state, $stateParams, ResourceUtility, RatingsEngineStore, Segments, QueryStore
) {
    var vm = this;

    angular.extend(vm, {
        stored: RatingsEngineStore.segment_form,
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
    	
    	vm.filteredSegments = vm.segments.slice(0, 15);

   		angular.forEach(vm.filteredSegments, function(segment) {

   			var segmentQuery = {
   				account_restriction: segment.account_restriction || [],
   				contact_restriction: segment.contact_restriction || [],
   				preexisting_segment_name: segment.name,
   				page_filter: {
   					num_rows: null,
   					row_offset: null
   				}
   			};
   			QueryStore.GetCountByQuery('accounts', segmentQuery).then(function(response){
				segment.num_accounts = response;
   			});
   			QueryStore.GetCountByQuery('contacts', segmentQuery).then(function(response){
				segment.num_contacts = response;
   			});

   		});

   		console.log($stateParams);

        RatingsEngineStore.setValidation('segment', false);
        if($stateParams.play_name) {
            RatingsEngineStore.setValidation('settings', true);
            RatingsEngineStore.getPlay($stateParams.play_name).then(function(play){
                vm.savedSegment = play.segment;
                vm.stored.segment_selection = play.segment;
                if(play.segment) {
                    RatingsEngineStore.setValidation('segment', true);
                }
            });
        }
    }

    vm.checkValidDelay = function(form) {
        $timeout(function() {
            vm.checkValid(form);
        }, 1);
    };

    vm.checkValid = function(form) {
        RatingsEngineStore.setValidation('segment', form.$valid);
        if(vm.stored.segment_selection) {
            RatingsEngineStore.setSettings({
                segment: vm.stored.segment_selection
            });
        }
    }

    vm.saveSegment = function(segment) {
        RatingsEngineStore.setSegment(segment);
    }

    vm.init();
});
