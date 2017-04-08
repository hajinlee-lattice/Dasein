angular.module('lp.models.segments', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.widgets.ModelDetailsWidget',
    'mainApp.models.modals.DeleteSegmentModal'
])
.controller('SegmentationListController', function ($scope, $rootScope, $element, $state, $stateParams, $timeout,
    ResourceUtility, Model, ModelStore, SegmentsList, DeleteSegmentModal, SegmentService) {

    var vm = this;
    angular.extend(vm, {
        modelId: $stateParams.modelId,
        tenantName: $stateParams.tenantName,
        model: Model,
        ResourceUtility: ResourceUtility,
        segments: SegmentsList
    });

    vm.init = function() {

        $rootScope.$broadcast('model-details',   { displayName: Model.ModelDetails.DisplayName });
        vm.Math = window.Math;

        $scope.showCustomMenu = false;

    };

    vm.init();

    vm.customMenuClick = function ($event, segment) {

        if ($event != null) {
            $event.stopPropagation();
        }

        segment.showCustomMenu = !segment.showCustomMenu;

        if (segment.showCustomMenu) {
            $(document).bind('click', function(event){
                var isClickedElementChildOfPopup = $element
                    .find(event.target)
                    .length > 0;

                if (isClickedElementChildOfPopup)
                    return;

                $scope.$apply(function(){
                    $scope.showCustomMenu = false;
                    $(document).unbind(event);
                });
            });
        }


    };

    vm.tileClick = function ($event, segmentName) {

        $event.preventDefault();
        $state.go('home.model.analysis', {segment: segmentName, create: false}, { reload: true } );

    };

    vm.editSegmentClick = function($event, segment){
        $event.stopPropagation();
        segment.showCustomMenu = !segment.showCustomMenu;
        segment.editSegment = !segment.editSegment;
    };

    vm.cancelEditSegmentClicked = function($event, segment) {
        $event.stopPropagation();
        segment.editSegment = !segment.editSegment;
    };

    vm.saveSegmentClicked = function($event, segment) {

        $event.stopPropagation();

        vm.saveInProgress = true;

        var segment = {
            name: segment.name,
            display_name: segment.display_name,
            description: segment.description
        };

        SegmentService.CreateOrUpdateSegment(segment).then(function(result) {

            var errorMsg = result.errorMsg;

            if (result.success) {
                console.log("success");
                $state.go('home.model.segmentation', {}, { reload: true });
            } else {
                console.log("error");
                vm.saveInProgress = false;
                vm.addSegmentErrorMessage = errorMsg;
                vm.showAddSegmentError = true;
            }
        });

    };

    vm.duplicateSegmentClick = function(){
    };

    vm.showDeleteSegmentModalClick = function($event, segment){

        $event.preventDefault();
        $event.stopPropagation();

        DeleteSegmentModal.show(segment);

    };

});