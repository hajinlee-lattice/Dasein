angular.module('mainApp.core.controllers.NoAssociationController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'common.utilities.browserstorage',
    'mainApp.appCommon.utilities.MetadataUtility'
])
.controller('NoAssociationController', function ($scope, ResourceUtility, BrowserStorageUtility, MetadataUtility) {

    var crmObject = BrowserStorageUtility.getSelectedCrmObject();
    var associationType = crmObject.associationType;
    
    $scope.supportEmail = null;
    $scope.supportEmailMessage = null;
    
    var customSettings = BrowserStorageUtility.getCrmCustomSettings();
    if (associationType === 'purchaseHistory') {
        // account does not exist in DanteAccount
        $scope.message1 = ResourceUtility.getString('NO_NOTION_FOUND_MESSAGE_1', ['SalesforceAccountID: ' + crmObject.ID]);
    } else {
        // Check the CRM custom settings and if it is configured then they will take precedence
        if (customSettings != null && customSettings.NoDataMessage != null) {
            // There is a configuration setting here that determines what the message is, 
            // but I'm hardcoding it for now to fix a critical item (COD-111)
            $scope.message1 = $scope.message1 || "No Recommendations Found";
        } else {
            $scope.message1 = $scope.message1 || ResourceUtility.getString('NO_ASSOCIATION_FOUND_MESSAGE_1');
        }
    }
    
    if (customSettings != null && customSettings.SupportEmail != null) {
        // There is probably a configuration setting here that determines what the email address is, 
        // but I'm hardcoding it for now to fix a critical item (COD-111)
        $scope.supportEmail = "support@lattice-engines.com";
        $scope.supportEmailMessage = ResourceUtility.getString('NO_ASSOCIATION_FOUND_SUPPORT_EMAIL');
    }
});