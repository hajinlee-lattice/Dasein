angular.module('lp.models.segments', [
    'mainApp.models.modals.DeleteSegmentModal'
])
.controller('SegmentationListController', function ($scope, $element, $state, $stateParams,
    SegmentsList, DeleteSegmentModal, SegmentService, QueryStore) {

    var vm = this;
    angular.extend(vm, {
        modelId: $stateParams.modelId,
        tenantName: $stateParams.tenantName,
        segments: SegmentsList || [],
        count: QueryStore.getCounts(),
        filteredItems: [],
        totalLength: SegmentsList.length,
        tileStates: {},
        query: '',
        header: {
            sort: {
                label: 'Sort By',
                icon: 'numeric',
                order: '-',
                property: 'created',
                items: [
                    { label: 'Creation Date',   icon: 'numeric',    property: 'created' },
                    { label: 'Segment Name',      icon: 'alpha',      property: 'display_name' }
                ]
            }
        }
    });


    vm.init = function($q) {
        
        console.log(vm.segments);

        SegmentsList.forEach(function(segment) {
            vm.tileStates[segment.name] = {
                showCustomMenu: false,
                editSegment: false
            };
        });

    }
    vm.init();

    vm.customMenuClick = function ($event, segment) {

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
            $state.go('home.segment.accounts', {segment: segment.name}, { reload: true } );
            //$state.go('home.segment.explorer.attributes', {segment: segment.name}, { reload: true } );
        } else {
            $state.go('home.model.analysis', {segment: segment.name}, { reload: true } );
        };
    };

    var oldSegmentDisplayName = '';
    var oldSegmentDescription = '';
    vm.editSegmentClick = function($event, segment){
        $event.stopPropagation();

        oldSegmentDescription = segment.description;
        oldSegmentDisplayName = segment.display_name;

        var tileState = vm.tileStates[segment.name];
        tileState.showCustomMenu = !tileState.showCustomMenu;
        tileState.editSegment = !tileState.editSegment;
    };

    vm.cancelEditSegmentClicked = function($event, segment) {
        $event.stopPropagation();

        segment.display_name = oldSegmentDisplayName;
        segment.description = oldSegmentDescription;
        oldSegmentDisplayName = '';
        oldSegmentDescription = '';

        var tileState = vm.tileStates[segment.name];
        tileState.editSegment = !tileState.editSegment;
    };

    vm.saveSegmentClicked = function($event, segment) {

        $event.stopPropagation();

        vm.saveInProgress = true;
        oldSegmentDisplayName = '';
        oldSegmentDescription = '';

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
                if ($state.current.name == 'home.segments') {
                    $state.go('home.segments', {}, { reload: true});
                } else {
                    $state.go('home.model.segmentation', {}, { reload: true });
                }
            } else {
                vm.saveInProgress = false;
                vm.addSegmentErrorMessage = errorMsg;
                vm.showAddSegmentError = true;
            }
        });

    }
});
