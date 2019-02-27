angular.module('mainApp.plays.controllers.NoPlaysController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'common.utilities.browserstorage'
])
.controller('NoPlaysController', function ($scope, ResourceUtility, BrowserStorageUtility) {
    
    $scope.showCustomMessage = false;
    $scope.noPlaysMessage = ResourceUtility.getString("DANTE_NO_PLAYS_LABEL");
    $scope.customMessage = "";
    
    // Check the CRM custom settings for Default Tab and if it is configured then they will take precedence
    var customSettings = BrowserStorageUtility.getCrmCustomSettings();
    if (customSettings != null && customSettings.NoPlaysMessage != null) {
        $scope.customMessage = customSettings.NoPlaysMessage;
        $scope.showCustomMessage = true;
    }
});