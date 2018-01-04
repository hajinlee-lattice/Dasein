angular.module('lp.segments.segments', [
    'mainApp.segments.modals.DeleteSegmentModal'
])
.controller('SegmentationListController', function ($scope, $element, $state, $stateParams,
    SegmentsList, DeleteSegmentModal, SegmentService, QueryStore, RatingsEngineStore) {

    var vm = this;
    angular.extend(vm, {
        modelId: $stateParams.modelId,
        tenantName: $stateParams.tenantName,
        segments: SegmentsList || [],
        count: QueryStore.getCounts(),
        filteredItems: [],
        totalLength: SegmentsList.length,
        tileStates: {},
        inEditing: {},
        query: '',
        header: {
            sort: {
                label: 'Sort By',
                icon: 'numeric',
                order: '-',
                property: 'updated',
                items: [
                    { label: 'Creation Date', icon: 'numeric', property: 'created' },
                    { label: 'Modified Date', icon: 'numeric', property: 'updated' },
                    { label: 'Author Name', icon: 'alpha', property: 'created_by' },
                    { label: 'Segment Name', icon: 'alpha', property: 'display_name' }
                ]
            }
        }
    });

    vm.init = function() {

        // console.log(vm.segments);

        vm.segmentIds = [];
        SegmentsList.forEach(function(segment) {
            vm.tileStates[segment.name] = {
                showCustomMenu: false,
                editSegment: false,
                saveEnabled: false
            };
            vm.segmentIds.push(segment.name);
        });
        // RatingsEngineStore.getSegmentsCounts(vm.segmentIds).then(function(response){
        //     console.log(response);
        // });

        if ($stateParams.edit) {
            var tileState = vm.tileStates[$stateParams.edit];
            tileState.editSegment = !tileState.editSegment;
            $stateParams.edit = null;
        }
    }

    vm.onInputFocus = function($event) {
        $event.target.select();
    };

    vm.customMenuClick = function($event, segment) {
        if ($event != null) {
            $event.stopPropagation();
        }

        var tileState = vm.tileStates[segment.name];
        tileState.showCustomMenu = !tileState.showCustomMenu

        if (tileState.showCustomMenu) {
            $(document).bind('click', function(event){
                var isClickedElementChildOfPopup = $element
                    .find(event.target)
                    .length > 0;

                if (isClickedElementChildOfPopup)
                    return;

                $scope.$apply(function(){
                    tileState.showCustomMenu = false;
                    $(document).unbind(event);
                });
            });
        }
    };

    vm.tileClick = function ($event, segment) {
        $event.preventDefault();
        if ($state.current.name == 'home.segments') {
            // $state.go('home.segment.accounts', {segment: segment.name}, { reload: true } );
            $state.go('home.segment.explorer.builder', {segment: segment.name}, { reload: true } );
        } else {
            $state.go('home.model.analysis', {segment: segment.name}, { reload: true } );
        };
    };

    vm.editSegmentClick = function($event, segment){
        $event.stopPropagation();
        vm.inEditing = angular.copy(segment);
        var tileState = vm.tileStates[segment.name];
        tileState.showCustomMenu = !tileState.showCustomMenu;
        tileState.editSegment = !tileState.editSegment;
    };

    vm.nameChanged = function(segment) {
        var tileState = vm.tileStates[segment.name];
        
        tileState.saveEnabled = !!(segment.display_name.length > 0);
    };

    vm.cancelEditSegmentClicked = function($event, segment) {
        if ($event) {
            $event.stopPropagation();
        }

        var tileState = vm.tileStates[segment.name];
        tileState.editSegment = !tileState.editSegment;
        segment.display_name = vm.inEditing.display_name || segment.display_name;
        segment.description = vm.inEditing.description || '';
        vm.inEditing = {};
    };

    vm.saveSegmentClicked = function($event, segment) {
        $event.stopPropagation();

        vm.saveInProgress = true;
        createOrUpdateSegment(segment);
        
    };

    vm.addSegment = function() {
        if (vm.modelId) {
            $state.go('home.model.analysis');
        } else {
            $state.go('home.segment.explorer.attributes', {segment: 'Create'});
        }
    };

    vm.duplicateSegmentClick = function($event, segment) {
        $event.preventDefault();
        $event.stopPropagation();

        vm.saveInProgress = true;
        segment.name = 'segment' + new Date().getTime();

        createOrUpdateSegment(segment);
    };

    vm.showDeleteSegmentModalClick = function($event, segment){

        $event.preventDefault();
        $event.stopPropagation();

        DeleteSegmentModal.show(segment, !!vm.modelId);

    };

    function createOrUpdateSegment(segment) {
        SegmentService.CreateOrUpdateSegment(segment).then(function(result) {

            var errorMsg = result.errorMsg;

            if (result.success) {
                var tileState = vm.tileStates[segment.name];
                tileState.editSegment = !tileState.editSegment;
                vm.saveInProgress = false;
                vm.showAddSegmentError = false;
                vm.inEditing = {};
            } else {
                vm.saveInProgress = false;
                vm.addSegmentErrorMessage = errorMsg;
                vm.showAddSegmentError = true;
            }
        });

    }

    vm.init();
});
