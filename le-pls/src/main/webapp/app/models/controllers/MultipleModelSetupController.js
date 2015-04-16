angular.module('mainApp.models.controllers.MultipleModelSetupController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.models.services.ModelService'
])

.controller('MultipleModelSetupController', function ($scope, BrowserStorageUtility, ResourceUtility, ModelService) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.loading = true;
    $scope.segments = [];
    if (BrowserStorageUtility.getClientSession() == null) { 
        return; 
    }
    
    var modelList = null;
    
    ModelService.GetAllModels(true).then(function(result) {
        // Get model list, but it may be empty
        if (result != null && result.success === true) {
            modelList = result.resultObj;
        }
        
        ModelService.GetAllSegments(modelList).then(function(result) {
            $scope.loading = false;
            if (result != null && result.success === true) {
                $scope.segments = result.resultObj;
            }
        });
    });
});