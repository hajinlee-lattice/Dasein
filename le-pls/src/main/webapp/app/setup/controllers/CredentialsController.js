angular.module('mainApp.setup.controllers.CredentialsController', [
    'mainApp.appCommon.directives.ngQtipDirective',
    'mainApp.appCommon.directives.helperMarkDirective',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.config.services.ConfigService',
    'mainApp.setup.services.TenantDeploymentService',
    'mainApp.core.controllers.SalesforceCredentialController'
])

.service('CredentialsService', function (StringUtility) {

    this.ValidateCredentials = function (type, credentials) {
        if (StringUtility.IsEmptyString(type) || credentials == null) {
            return false;
        }
        var isValid = false;
        switch (type) {
            case "sfdc":
                isValid = !StringUtility.IsEmptyString(credentials.UserName) && !StringUtility.IsEmptyString(credentials.Password);
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

.controller('CredentialsController', function ($scope, $rootScope, ResourceUtility, BrowserStorageUtility, NavUtility, ConfigService, TenantDeploymentService, CredentialsService) {
    $scope.ResourceUtility = ResourceUtility;

    $scope.loading = true;
    $scope.showError = false;
    $scope.errorMessage = "";

    var editText = ResourceUtility.getString('BUTTON_EDIT_LABEL');
    var importText = ResourceUtility.getString('BUTTON_IMPORT_LABEL');
    var saveAndImportText = ResourceUtility.getString('SETUP_CREDENTIALS_SAVE_IMPORT_BUTTON');
    $scope.sfdcGuideName = ResourceUtility.getString('SETUP_CREDENTIALS_SF_CONFIGURATION_GUIDE_LABEL');
    $scope.logoLeftAlign = true;
    $scope.crmProductionComplete = false;
    $scope.crmProductionError = "";
    $scope.crmProductionSaveInProgress = false;
    $scope.crmProductionSaveButtonText = saveAndImportText;
    $scope.crmProductionEditButtonText = editText;
    $scope.crmProductionImportButtonText = importText;
    $scope.crmSandboxComplete = false;
    $scope.crmSandboxError = "";
    $scope.crmSandboxSaveInProgress = false;
    $scope.crmSandboxSaveButtonText = saveAndImportText;
    $scope.crmSandboxEditButtonText = editText;
    $scope.crmSandboxImportButtonText = importText;

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
            if (typeof result.resultObj === "string" && result.resultObj.toLowerCase() === "marketo") {
                $scope.isMarketo = true;
            }
            if (typeof result.resultObj === "string" && result.resultObj.toLowerCase() === "eloqua") {
                $scope.isEloqua = true;
            }
            if ($scope.isMarketo || $scope.isEloqua) {
                var mapType = $scope.isMarketo ? "marketo" : "eloqua";
                ConfigService.GetCurrentCredentials(mapType).then(function(result) {
                    if (result != null && result.success === true) {
                        var returned = result.resultObj;
                        $scope.mapCredentials = new Credentials(returned.Url, returned.SecurityToken, returned.OrgId, returned.Password, returned.UserName, returned.Company);
                        $scope.mapComplete = true;
                    } else {
                        $scope.mapCredentials = new Credentials();
                    }
                    getSFDCCredentials();
                });
            } else {
                getSFDCCredentials();
            }
        } else {
            $scope.showError = true;
            $scope.errorMessage = result.resultErrors;
            getSFDCCredentials();
        }
    });

    function getSFDCCredentials() {
        ConfigService.GetCurrentCredentials("sfdc", true).then(function(result) {
            if (result != null && result.success === true) {
                var returned = result.resultObj;
                $scope.crmProductionCredentials = new Credentials(null, returned.SecurityToken, null, returned.Password, returned.UserName, null);
                $scope.crmProductionComplete = true;
            } else {
                $scope.crmProductionCredentials = new Credentials();
            }

            ConfigService.GetCurrentCredentials("sfdc", false).then(function(result) {
                if (result != null && result.success === true) {
                    var returned = result.resultObj;
                    $scope.crmSandboxCredentials = new Credentials(null, returned.SecurityToken, null, returned.Password, returned.UserName, null);
                    $scope.crmSandboxComplete = true;
                } else {
                    $scope.crmSandboxCredentials = new Credentials();
                }
            });
        });
    }

    $scope.crmProductionSaveAndImportClicked = function () {
        $scope.crmProductionError = "";
        if (CredentialsService.ValidateCredentials("sfdc", $scope.crmProductionCredentials)) {
            if ($scope.crmProductionSaveInProgress) {
                return;
            }
            $scope.crmProductionSaveInProgress = true;
            ConfigService.ValidateApiCredentials("sfdc", $scope.crmProductionCredentials, true).then(function(result) {
                if (result == null) {
                    $scope.crmProductionSaveInProgress = false;
                    $scope.crmProductionError = ResourceUtility.getString("SYSTEM_ERROR");
                } else if (result.success === true) {
                    $scope.crmProductionComplete = true;
                    $scope.crmProductionImportClicked();
                } else {
                    $scope.crmProductionSaveInProgress = false;
                    $scope.crmProductionError = result.resultErrors;
                }
            });
        } else {
            $scope.crmProductionError = ResourceUtility.getString("SYSTEM_SETUP_REQUIRED_FIELDS_ERROR");
        }
    };

    $scope.crmSandboxSaveAndImportClicked = function () {
        $scope.crmSandboxError = "";
        if (CredentialsService.ValidateCredentials("sfdc", $scope.crmSandboxCredentials)) {
            if ($scope.crmSandboxSaveInProgress) {
                return;
            }
            $scope.crmSandboxSaveInProgress = true;
            ConfigService.ValidateApiCredentials("sfdc", $scope.crmSandboxCredentials, false).then(function(result) {
                if (result == null) {
                    $scope.crmSandboxSaveInProgress = false;
                    $scope.crmSandboxError = ResourceUtility.getString("SYSTEM_ERROR");
                } else if (result.success === true) {
                    $scope.crmSandboxComplete = true;
                    $scope.crmSandboxImportClicked();
                } else {
                    $scope.crmSandboxSaveInProgress = false;
                    $scope.crmSandboxError = result.resultErrors;
                }
            });
        } else {
            $scope.crmSandboxError = ResourceUtility.getString("SYSTEM_SETUP_REQUIRED_FIELDS_ERROR");
        }
    };

    $scope.crmProductionImportClicked = function () {
        importSfdcData(true);
    };

    $scope.crmSandboxImportClicked = function () {
        importSfdcData(false);
    };

    function importSfdcData(isProduction) {
        if (isProduction) {
            $scope.crmProductionSaveInProgress = true;
        } else {
            $scope.crmSandboxSaveInProgress = true;
        }
        TenantDeploymentService.ImportSfdcData().then(function (result){
            if (result.Success === true) {
                $rootScope.$broadcast(NavUtility.DEPLOYMENT_WIZARD_NAV_EVENT);
            } else {
                if (isProduction) {
                    $scope.crmProductionSaveInProgress = false;
                    $scope.crmProductionError = result.ResultErrors;
                } else {
                    $scope.crmSandboxSaveInProgress = false;
                    $scope.crmSandboxError = result.ResultErrors;
                }
            }
        });
    }

})

.directive('credentials', function () {
    return {
        templateUrl: 'app/setup/views/CredentialsView.html'
    };
});