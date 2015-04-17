angular.module('mainApp.models.controllers.MultipleModelSetupController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.models.services.ModelService',
    'mainApp.models.modals.AddSegmentModal'
])

.controller('MultipleModelSetupController', function ($scope, BrowserStorageUtility, ResourceUtility, ModelService, AddSegmentModal) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.loading = true;
    $scope.segments = [];
    if (BrowserStorageUtility.getClientSession() == null) { 
        return; 
    }
    
    var models = null;
    
    ModelService.GetAllModels(true).then(function(result) {
        // Get model list, but it may be empty
        if (result != null && result.success === true) {
            models = result.resultObj;
        }
        
        ModelService.GetAllSegments(models).then(function(result) {
            $scope.loading = false;
            if (result != null && result.success === true) {
                $scope.segments = result.resultObj;
            } else {
                // Need to handle error case
            }
        });
    });
    
    $scope.addNewSegmentClicked = function () {
        AddSegmentModal.show($scope.segments, models, function (segment) {
            if (segment != null) {
                $scope.segments.push(segment);
            }
        });
    };
    
    $scope.deleteSegmentClicked = function (segment) {
        if (segment == null) {
            return;
        }
        ModelService.DeleteSegment(segment.Name).then(function(result) {
            if (result != null && result.success === true) {
                for (var i=0;i<$scope.segments.length;i++) {
                    if (segment.Name == $scope.segments[i].Name) {
                        $scope.segments.splice(i, 1);
                        break;
                    }
                }
            } else {
                // Need to handle error case
            }
        });
        
    };
});