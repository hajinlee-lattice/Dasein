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
                isValid = !StringUtility.IsEmptyString(credentials.UserName) && !StringUtility.IsEmptyString(credentials.Password) &&
                    !StringUtility.IsEmptyString(credentials.Company);
                break;
            case "marketo":
                isValid = !StringUtility.IsEmptyString(credentials.UserName) && !StringUtility.IsEmptyString(credentials.Password) &&
                    !StringUtility.IsEmptyString(credentials.Url);
                break;
        }
        return isValid;
    };
})

.controller('ManageCredentialsController', function ($scope, $rootScope, ResourceUtility, BrowserStorageUtility, ConfigService, ManageCredentialsService) {
    $scope.ResourceUtility = ResourceUtility;
    
    $scope.crmProductionComplete = false;
    $scope.crmProductionError = "";
    $scope.crmProductionSaveInProgress = false;
    
    $scope.crmSandboxComplete = false;
    $scope.crmSandboxError = "";
    $scope.crmSandboxSaveInProgress = false;
    
    $scope.mapComplete = false;
    $scope.mapError = "";
    $scope.mapSaveInProgress = false;
    
    $scope.loading = true;
    $scope.showError = false;
    $scope.errorMessage = "";
    
    $scope.closeErrorClick = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        
        $scope.showError = false;
    };
    
    function Credentials (url, securitytoken, orgid, password, username, company) {
        this.Url = url || null;
        this.SecurityToken = securitytoken || null;
        this.OrgId = orgid || null;
        this.Password = password || null;
        this.UserName = username || null;
        this.Company = company || null;
    }
    
    ConfigService.GetCurrentTopology().then(function(result) {
        $scope.loading = false;
        if (result.success === true) {
            if (typeof result.resultObj === "string" && result.resultObj.toLowerCase() === "eloqua") {
                $scope.isMarketo = false;
            } else {
                $scope.isMarketo = true;
            }
            var mapType = $scope.isMarketo ? "marketo" : "eloqua";
            ConfigService.GetCurrentCredentials(mapType).then(function(result) {
                if (result != null && result.success === true) {
                    var returned = result.resultObj;
                    $scope.mapCredentials = new Credentials(returned.Url, returned.SecurityToken, returned.OrgId, returned.Password, returned.UserName, returned.Company);
                    $scope.mapComplete = true;
                } else {
                    $scope.mapCredentials = new Credentials();
                }
            });
        } else {
            $scope.showError = true;
            $scope.errorMessage = result.resultErrors;
        }
    });
    
    ConfigService.GetCurrentCredentials("sfdc", true).then(function(result) {
        if (result != null && result.success === true) {
            var returned = result.resultObj;
            $scope.crmProductionCredentials = new Credentials(null, returned.SecurityToken, null, returned.Password, returned.UserName, null);
            $scope.crmProductionComplete = true;
        } else {
            $scope.crmProductionCredentials = new Credentials();
        }
    });
    
    ConfigService.GetCurrentCredentials("sfdc", false).then(function(result) {
        if (result != null && result.success === true) {
            var returned = result.resultObj;
            $scope.crmSandboxCredentials = new Credentials(null, returned.SecurityToken, null, returned.Password, returned.UserName, null);
            $scope.crmSandboxComplete = true;
        } else {
            $scope.crmSandboxCredentials = new Credentials();
        }
    });
    
    $scope.crmProductionSaveClicked = function () {
        $scope.crmProductionError = "";
        if (ManageCredentialsService.ValidateCredentials("sfdc", $scope.crmProductionCredentials)) {
            if ($scope.crmProductionSaveInProgress) {
                return;
            }
            $scope.crmProductionSaveInProgress = true;
            ConfigService.ValidateApiCredentials("sfdc", $scope.crmProductionCredentials, true).then(function(result) {
                $scope.crmProductionSaveInProgress = false;
                if (result == null) {
                    $scope.crmProductionError = ResourceUtility.getString("SYSTEM_ERROR");
                } else if (result.success === true) {
                    $scope.crmProductionComplete = true;
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
            if ($scope.crmSandboxSaveInProgress) {
                return;
            }
            $scope.crmSandboxSaveInProgress = true;
            ConfigService.ValidateApiCredentials("sfdc", $scope.crmSandboxCredentials, false).then(function(result) {
                $scope.crmSandboxSaveInProgress = false;
                if (result == null) {
                    $scope.crmSandboxError = ResourceUtility.getString("SYSTEM_ERROR");
                } else if (result.success === true) {
                    $scope.crmSandboxComplete = true;
                } else {
                    $scope.crmSandboxError = result.resultErrors;
                }
            });
        } else {
            $scope.crmSandboxError = ResourceUtility.getString("SYSTEM_SETUP_REQUIRED_FIELDS_ERROR");
        }
    };
    
    $scope.marketoSaveClicked = function () {
        $scope.mapError = "";
        if (ManageCredentialsService.ValidateCredentials("marketo", $scope.mapCredentials)) {
            if ($scope.mapSaveInProgress) {
                return;
            }
            $scope.mapSaveInProgress = true;
            ConfigService.ValidateApiCredentials("marketo", $scope.mapCredentials, false).then(function(result) {
                $scope.mapSaveInProgress = false;
                if (result == null) {
                    $scope.mapError = ResourceUtility.getString("SYSTEM_ERROR");
                } else if (result.success === true) {
                    $scope.mapComplete = true;
                } else {
                    $scope.mapError = result.resultErrors;
                }
            });
        } else {
            $scope.mapError = ResourceUtility.getString("SYSTEM_SETUP_REQUIRED_FIELDS_ERROR");
        }
    };
    
    $scope.eloquaSaveClicked = function () {
        $scope.mapError = "";
        if (ManageCredentialsService.ValidateCredentials("eloqua", $scope.mapCredentials)) {
            if ($scope.mapSaveInProgress) {
                return;
            }
            $scope.mapSaveInProgress = true;
            ConfigService.ValidateApiCredentials("eloqua", $scope.mapCredentials, false).then(function(result) {
                $scope.mapSaveInProgress = false;
                if (result == null) {
                    $scope.mapError = ResourceUtility.getString("SYSTEM_ERROR");
                } else if (result.success === true) {
                    $scope.mapComplete = true;
                } else {
                    $scope.mapError = result.resultErrors;
                }
            });
        } else {
            $scope.mapError = ResourceUtility.getString("SYSTEM_SETUP_REQUIRED_FIELDS_ERROR");
        }
    };
    
});