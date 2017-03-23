angular.module('lp.models.segments', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.widgets.ModelDetailsWidget',
    'mainApp.models.modals.DeleteSegmentModal'
])
.controller('SegmentationListController', function ($scope, $rootScope, $state, $stateParams, $timeout, 
    ResourceUtility, Model, ModelStore, SegmentsList, SegmentService) {

    var DummySegment = {
  "name": "Test",
  "description": null,
  "updated": 1490197173235,
  "created": 1490197173235,
  "restriction": null,
  "display_name": "Test",
  "simple_restriction": {
    "any": [],
    "all": [
      {
        "bucketRestriction": {
          "lhs": {
            "columnLookup": {
              "column_name": "TechIndicator_AdRoll",
              "object_type": "BucketedAccountMaster"
            }
          },
          "range": {
            "min": "Yes",
            "max": "Yes",
            "is_null_only": false
          }
        }
      }
    ]
  },
  "segment_properties": [
    {
      "metadataSegmentProperty": {
        "option": "NumAccounts",
        "value": "1435"
      }
    }
  ]
}

    var vm = this;
    angular.extend(vm, {
        modelId: $stateParams.modelId,
        tenantName: $stateParams.tenantName,
        model: Model,
        ResourceUtility: ResourceUtility,
        segments: DummySegment,
        editSegment: false,
        showCustomMenu: false
    });

    vm.init = function() {
        $rootScope.$broadcast('model-details',   { displayName: Model.ModelDetails.DisplayName });
        vm.Math = window.Math;
        $scope.nameStatus = {
            editing: false
        };

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
        console.log("segment");
    };

    vm.editSegmentClick = function($event){
        $event.stopPropagation();
        console.log("edit");
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