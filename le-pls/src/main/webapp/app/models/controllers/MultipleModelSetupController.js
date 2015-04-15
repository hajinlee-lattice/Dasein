angular.module('mainApp.models.controllers.MultipleModelSetupController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility'
])

.controller('MultipleModelSetupController', function ($scope, BrowserStorageUtility, ResourceUtility) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.loading = true;

    if (BrowserStorageUtility.getClientSession() == null) { 
        return; 
    }

});