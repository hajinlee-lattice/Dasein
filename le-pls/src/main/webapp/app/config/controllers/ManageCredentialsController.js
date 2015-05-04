angular.module('mainApp.config.controllers.ManageCredentialsController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.config.services.ConfigService'
])

.service('ManageCredentialsService', function (StringUtility) {

    this.ValidateCredentials = function (type, credentials) {
        if (StringUtility.IsEmptyString(type) || credentials == null) {
            return false;
        }
        var isValid = false;
        switch (type) {
            case "sfdc":
                isValid = !StringUtility.IsEmptyString(credentials.UserName) && !StringUtility.IsEmptyString(credentials.Password) &&
                    !StringUtility.IsEmptyString(credentials.SecurityToken);
                break;
            case "eloqua":
                isValid = !StringUtility.IsEmptyString(credentials.UserName) && !StringUtility.IsEmptyString(credentials.SecurityToken) &&
                    !StringUtility.IsEmptyString(credentials.Url);
                break;
            case "marketo":
                isValid = !StringUtility.IsEmptyString(credentials.UserName) && !StringUtility.IsEmptyString(credentials.Password) &&
                    !StringUtility.IsEmptyString(credentials.SecurityToken);
                break;
        }
        return isValid;
    };
})

.controller('ManageCredentialsController', function ($scope, $rootScope, ResourceUtility, BrowserStorageUtility, ConfigService, ManageCredentialsService) {
    $scope.ResourceUtility = ResourceUtility;
    
    $scope.crmProductionComplete = false;
    $scope.crmProductionError = "";
    var crmProductionSaveInProgress = false;
    
    $scope.crmSandboxComplete = false;
    $scope.crmSandboxError = "";
    var crmSandboxSaveInProgress = false;
    
    $scope.mapComplete = false;
    $scope.mapError = "";
    var mapSaveInProgress = false;
    
    $scope.loading = true;
    
    function Credentials (url, securitytoken, orgid, password, username, company) {
        this.Url = url || null;
        this.SecurityToken = securitytoken || null;
        this.OrgId = orgid || null;
        this.Password = password || null;
        this.UserName = username || null;
        this.Company = company || null;
    }
    
    //TODO:pierce this will have to be changed once I can actually check existing credentials
    $scope.loading = false;
    $scope.crmSandboxCredentials = new Credentials();
    $scope.crmProductionCredentials = new Credentials();
    $scope.mapCredentials = new Credentials();
    $scope.isMarketo = true;
    ConfigService.GetCurrentCredentials("sfdc").then(function(result) {
        if (result != null && result.success === true) {
            
        }
    });
    
    $scope.crmProductionSaveClicked = function () {
        $scope.crmProductionError = "";
        if (ManageCredentialsService.ValidateCredentials("sfdc", $scope.crmProductionCredentials)) {
            if (crmProductionSaveInProgress) {
                return;
            }
            crmProductionSaveInProgress = true;
            ConfigService.ValidateApiCredentials("sfdc", $scope.crmProductionCredentials, true).then(function(result) {
                crmProductionSaveInProgress = false;
                if (result == null) {
                    $scope.crmProductionError = ResourceUtility.getString("SYSTEM_ERROR");
                } else if (result.success === true) {
                    
                } else {
                    $scope.crmProductionError = result.resultErrors;
                }
            });
        } else {
            $scope.crmProductionError = ResourceUtility.getString("SYSTEM_SETUP_REQUIRED_FIELDS_ERROR");
        }
    };
    
    $scope.crmSandboxSaveClicked = function () {
        $scope.crmSandboxError = "";
        if (ManageCredentialsService.ValidateCredentials("sfdc", $scope.crmSandboxCredentials)) {
            if (crmSandboxSaveInProgress) {
                return;
            }
            crmSandboxSaveInProgress = true;
            ConfigService.ValidateApiCredentials("sfdc", $scope.crmSandboxCredentials, false).then(function(result) {
                crmSandboxSaveInProgress = false;
                if (result == null) {
                    $scope.crmSandboxError = ResourceUtility.getString("SYSTEM_ERROR");
                } else if (result.success === true) {
                    
                } else {
                    $scope.crmSandboxError = result.resultErrors;
                }
            });
        } else {
            $scope.crmSandboxError = ResourceUtility.getString("SYSTEM_SETUP_REQUIRED_FIELDS_ERROR");
        }
    };
    
});