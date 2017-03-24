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

        $event.stopPropagation();

    };

    vm.tileClick = function ($event, segmentName) {

        $event.preventDefault();
        $state.go('home.model.analysis', segmentName, { reload: true } );

    };

    vm.editSegmentClick = function($event, segmentName){
        $event.stopPropagation();
        
        console.log($event.currentTarget);

        vm.editSegment = true;
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

    vm.showDeleteSegmentModalClick = function($event){
        $event.preventDefault();
        DeleteSegmentModal.show();
    };

});