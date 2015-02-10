angular.module('mainApp.config.controllers.ManageCredentialsController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.config.modals.EnterCredentialsModal',
    'mainApp.core.utilities.GriotNavUtility'
])

.controller('ManageCredentialsController', function ($scope, $rootScope, ResourceUtility, BrowserStorageUtility, GriotNavUtility, EnterCredentialsModal) {
    $scope.ResourceUtility = ResourceUtility;
    
    $scope.crmCredentialsCompleteClass = "";
    $scope.mapCredentialsCompleteClass = "";
    
    var configDoc = BrowserStorageUtility.getConfigDocument();
    if (configDoc != null) {
        $scope.crmCredentialsCompleteClass = configDoc.CrmApiCredentials != null ? "active" : "";
        $scope.mapCredentialsCompleteClass = configDoc.MapApiCredentials != null ? "active" : "";
    }
    
    $scope.enterCrmCredentialsClicked = function () {
        EnterCredentialsModal.show(configDoc.CrmType, configDoc.CrmApiCredentials, function (apiCredentials) {
            $scope.crmCredentialsCompleteClass = "active";
            configDoc.CrmApiCredentials = apiCredentials;
            BrowserStorageUtility.setConfigDocument(configDoc);
            
            checkIfSystemCredentialsComplete();
        });
    };
    
    $scope.enterMapCredentialsClicked = function () {
        EnterCredentialsModal.show(configDoc.MapType, configDoc.MapApiCredentials, function (apiCredentials) {
            $scope.mapCredentialsCompleteClass = "active";
            configDoc.MapApiCredentials = apiCredentials;
            BrowserStorageUtility.setConfigDocument(configDoc);
            
            checkIfSystemCredentialsComplete();
        });
    };
    
    function checkIfSystemCredentialsComplete () {
        if (configDoc != null && configDoc.CrmApiCredentials != null && configDoc.MapApiCredentials != null) {
            $rootScope.$broadcast(GriotNavUtility.SYSTEM_CONFIGURED_COMPLETE_EVENT);
        }
    }
    
});