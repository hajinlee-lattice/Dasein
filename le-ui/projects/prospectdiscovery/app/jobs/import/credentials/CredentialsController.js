angular.module('controllers.jobs.import.credentials', [
    'mainApp.appCommon.directives.ngQtipDirective',
    'mainApp.appCommon.directives.helperMarkDirective',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.config.services.ConfigService'
])

.service('CredentialsService', function (StringUtility) {

    this.ValidateCredentials = function (type, credentials) {
        console.log('ValidateCredentials');

        if (StringUtility.IsEmptyString(type) || credentials == null) {
            console.log('credentials == null');
            return false;
        }

        var isValid = false;
        switch (type) {
            case "sfdc":
                isValid = !StringUtility.IsEmptyString(credentials.UserName) && !StringUtility.IsEmptyString(credentials.Password);
                break;
        }
        return isValid;
    };
})

.controller('CredentialsCtrl', function ($scope, $rootScope, $location, ResourceUtility, BrowserStorageUtility, ConfigService, CredentialsService) {
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
    $scope.loading = false;
    getSFDCCredentials();

    /*
    ConfigService.GetCurrentTopology().then(function(result) {
        console.log('GetCurrentTopology', result);
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
                    console.log('GetCurrentCredentials', result);
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
    */
    function getSFDCCredentials() {
        console.log('getSFDCCredentials');
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
    
    $scope.crmProductionSaveClicked = function () {
        $scope.crmProductionError = "";
        if (CredentialsService.ValidateCredentials("sfdc", $scope.crmProductionCredentials)) {
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
                    $scope.createDefaultTargetMarket();
                } else {
                    $scope.crmProductionError = result.resultErrors;
                }
            });
        } else {
            $scope.crmProductionError = ResourceUtility.getString("SYSTEM_SETUP_REQUIRED_FIELDS_ERROR");
        }
    };

    // FIXME - Move this to the MarketsService file when it's created
    $scope.createDefaultTargetMarket = function () {
        var authorizationToken = BrowserStorageUtility.getTokenDocument();
        $.ajax({
            url: '/pls/targetmarkets/default',
            method: 'POST',
            beforeSend: function(xhr){
                xhr.setRequestHeader('Authorization', authorizationToken);
            },
            complete: function(event, status) {
                console.log(event);
                
                if (status != "success") {
                    var msg = event.responseJSON.errorMsg ? event.responseJSON.errorMsg : '';
                    alert('Default Target Market STATUS: ' + status + '\nMESSAGE:' + msg);
                }

                window.location.hash = '#/jobs/status';
            }
        });
    };

    $scope.crmSandboxSaveClicked = function () {
        $scope.crmSandboxError = "";
        if (CredentialsService.ValidateCredentials("sfdc", $scope.crmSandboxCredentials)) {
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
});