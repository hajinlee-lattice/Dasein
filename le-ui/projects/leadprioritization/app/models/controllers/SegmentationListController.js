angular.module('lp.models.segments', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.widgets.ModelDetailsWidget'
])
.controller('SegmentationListController', function ($scope, $rootScope, $state, $stateParams, $timeout, 
    ResourceUtility, Model, ModelStore, SegmentService) {

    var vm = this;
    angular.extend(vm, {
        modelId: $stateParams.modelId,
        tenantName: $stateParams.tenantName,
        model: Model,
        ResourceUtility: ResourceUtility,
        segments: SegmentsList,
        editSegment: false,
        showCustomMenu: false
    });

    vm.init = function() {
        $rootScope.$broadcast('model-details',   { displayName: Model.ModelDetails.DisplayName });
        vm.Math = window.Math;

        console.log(vm.segments);
    
    };

    vm.init();

    vm.customMenuClick = function ($event) {
        if ($event != null) {
            $event.stopPropagation();
        }
        
        $scope.showCustomMenu = !$scope.showCustomMenu;

        if ($scope.showCustomMenu) {
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

    vm.tileClick = function ($event) {
        $event.preventDefault();
    };

    vm.editSegmentClick = function(){
        vm.editSegment = true;

        // Show edit segment form

    };

    vm.cancelEditSegmentClicked = function() {
        vm.editSegment = false;
    };

    vm.saveSegmentClicked = function() {

        vm.saveInProgress = true;

        var segment = {
            segmentName: vm.segmentName,
            segmentDescription: vm.segmentDescription
        };

        SegmentService.UpdateSegment(credential).then(function(result) {
            
            var errorMsg = result.errorMsg;

            if (result.success) {
                $state.go('home.model.segmentation');
            } else {
                vm.saveInProgress = false;
                vm.addSegmentErrorMessage = errorMsg;
                vm.showAddSegmentError = true;
            }
        });
    };

    vm.duplicateSegmentClick = function(){
        // Need help from Jamey on this one
    };

    vm.deleteSegmentClick = function(){
        // Open model for confirm
    };

});