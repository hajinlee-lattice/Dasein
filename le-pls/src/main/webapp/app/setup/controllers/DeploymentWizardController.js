angular.module('mainApp.setup.controllers.DeploymentWizardController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.setup.utilities.SetupUtility',
    'mainApp.setup.controllers.CredentialsController',
    'mainApp.setup.controllers.ImportAndEnrichDataController',
    'mainApp.setup.controllers.ManageFieldsController',
    'mainApp.setup.services.TenantDeploymentService',
    'mainApp.setup.services.MetadataService'
])

.controller('DeploymentWizardController', function ($scope, $rootScope, ResourceUtility, BrowserStorageUtility, SetupUtility, TenantDeploymentService) {
    $scope.ResourceUtility = ResourceUtility;
    if (BrowserStorageUtility.getClientSession() == null) { return; }

    $scope.importTab = new Tab(ResourceUtility.getString('SETUP_DEPLOYMENT_WIZARD_NAV_IMPORT_AND_ENRICH_DATA'), false, true);
    $scope.buildTab = new Tab(ResourceUtility.getString('SETUP_DEPLOYMENT_WIZARD_NAV_BUILD_MODEL'), false, true);
    $scope.publishTab = new Tab(ResourceUtility.getString('SETUP_DEPLOYMENT_WIZARD_NAV_PUBLISH_SCORES'), false, true);
    $scope.tabs = [$scope.importTab, $scope.buildTab, $scope.publishTab];

    $scope.loading = true;
    TenantDeploymentService.GetTenantDeployment().then(function(result) {
        $scope.loading = false;
        if (result.Success) {
            $scope.deployment = result.ResultObj;
            if ($scope.deployment == null || $scope.deployment.Step === SetupUtility.STEP_ENTER_CREDENTIALS) {
                $scope.importTab.active = true;
                $scope.importTab.disabled = false;
                $scope.showEnterCredentials = true;
            } else if ($scope.deployment.Step === SetupUtility.STEP_IMPORT_DATA ||
                    $scope.deployment.Step === SetupUtility.STEP_ENRICH_DATA ||
                    $scope.deployment.Step === SetupUtility.STEP_VALIDATE_METADATA) {
                $scope.importTab.active = true;
                $scope.importTab.disabled = false;
                $scope.showImportAndEnrichData = true;
            }
        } else {
            $scope.getDeploymentError = result.ResultErrors;
        }
    });

    $scope.tabClicked = function ($event, tab) {
        if ($event != null) {
            $event.preventDefault();
        }

        if (!tab.disabled) {
            for (var i = 0; i < $scope.tabs.length; i++) {
                $scope.tabs[i].active = false;
            }
            tab.active = true;
        }
    };

    function Tab(title, active, disabled) {
        var tab = {};
        tab.title = title;
        tab.active = active;
        tab.disabled = disabled;
        return tab;
    }
});