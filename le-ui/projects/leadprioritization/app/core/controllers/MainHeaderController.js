angular.module('mainApp.core.controllers.MainHeaderController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.login.services.LoginService',
    'mainApp.core.services.FeatureFlagService'
])

.controller('MainHeaderController', function ($scope, $rootScope, ResourceUtility, BrowserStorageUtility, NavUtility, LoginService, FeatureFlagService) {
    $scope.ResourceUtility = ResourceUtility;

    /*
    var clientSession = BrowserStorageUtility.getClientSession();
    if (clientSession != null) {
        FeatureFlagService.GetAllFlags().then(function() {
            var flags = FeatureFlagService.Flags();
            $scope.userDisplayName = clientSession.DisplayName;
            $scope.showUserManagement = FeatureFlagService.FlagIsEnabled(flags.USER_MGMT_PAGE);
            $scope.showSystemSetup = FeatureFlagService.FlagIsEnabled(flags.SYSTEM_SETUP_PAGE);
            $scope.showModelCreationHistoryDropdown = FeatureFlagService.FlagIsEnabled(flags.MODEL_HISTORY_PAGE);
            $scope.showActivateModel = FeatureFlagService.FlagIsEnabled(flags.ACTIVATE_MODEL_PAGE);
            $scope.showSetup = FeatureFlagService.FlagIsEnabled(flags.SETUP_PAGE);
            $scope.showDeploymentWizard = FeatureFlagService.FlagIsEnabled(flags.DEPLOYMENT_WIZARD_PAGE);
            $scope.redirectToDeploymentWizard = FeatureFlagService.FlagIsEnabled(flags.REDIRECT_TO_DEPLOYMENT_WIZARD_PAGE);
            $scope.showLeadEnrichment = FeatureFlagService.FlagIsEnabled(flags.LEAD_ENRICHMENT_PAGE);
        });
    }
    */

    checkBrowserWidth();
    $(window).resize(checkBrowserWidth);

    $scope.handleSidebarToggle = function ($event) {
        $("body").toggleClass("open-nav");
    }

    function checkBrowserWidth(){
        if (window.matchMedia("(min-width: 1200px)").matches) {
            $("body").addClass("open-nav");
        } else {
            $("body").removeClass("open-nav");
        }
    }
});