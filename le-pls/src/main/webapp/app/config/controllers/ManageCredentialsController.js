angular.module('mainApp.config.controllers.ManageCredentialsController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.GriotNavUtility'
])

.controller('ManageCredentialsController', function ($scope, $rootScope, ResourceUtility, BrowserStorageUtility, GriotNavUtility) {
    $scope.ResourceUtility = ResourceUtility;
    
    $scope.crmCredentialsCompleteClass = "";
    $scope.mapCredentialsCompleteClass = "";
    
    var configDoc = BrowserStorageUtility.getConfigDocument();
    if (configDoc != null) {
        $scope.crmCredentialsCompleteClass = configDoc.CrmApiCredentials != null ? "active" : "";
        $scope.mapCredentialsCompleteClass = configDoc.MapApiCredentials != null ? "active" : "";
    }
    
    $scope.enterCrmCredentialsClicked = function () {
        
    };
    
    $scope.enterMapCredentialsClicked = function () {
        
    };
    
    function checkIfSystemCredentialsComplete () {
        if (configDoc != null && configDoc.CrmApiCredentials != null && configDoc.MapApiCredentials != null) {
            $rootScope.$broadcast(GriotNavUtility.SYSTEM_CONFIGURED_COMPLETE_EVENT);
        }
    }
    
});