angular.module('mainApp.config.controllers.ManageCredentialsController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.config.modals.EnterCredentialsModal'
])

.controller('ManageCredentialsController', function ($scope, $rootScope, ResourceUtility, BrowserStorageUtility, NavUtility, EnterCredentialsModal) {
    $scope.ResourceUtility = ResourceUtility;
    
    $scope.crmCredentialsCompleteClass = "";
    $scope.mapCredentialsCompleteClass = "";
    
    var configDoc = BrowserStorageUtility.getConfigDocument();
    if (configDoc != null) {
        $scope.crmCredentialsCompleteClass = configDoc.CrmApiCredentials != null ? "active" : "";
        $scope.mapCredentialsCompleteClass = configDoc.MapApiCredentials != null ? "active" : "";
    }
    
    $scope.enterCrmCredentialsClicked = function () {
        EnterCredentialsModal.show("SFDC", null, function () {
            
        });
    };
    
    $scope.enterMapCredentialsClicked = function () {
        EnterCredentialsModal.show("Marketo", null, function () {
            
        });
    };
    
    function checkIfSystemCredentialsComplete () {
        if (configDoc != null && configDoc.CrmApiCredentials != null && configDoc.MapApiCredentials != null) {
            $rootScope.$broadcast(NavUtility.SYSTEM_CONFIGURED_COMPLETE_EVENT);
        }
    }
    
});