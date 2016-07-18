angular
    .module('pd.navigation.sidebar', [
        'mainApp.appCommon.utilities.ResourceUtility',
        'mainApp.appCommon.utilities.StringUtility',
        'mainApp.core.services.FeatureFlagService'
    ])
    .controller('SidebarRootController', function(
        $scope, $state, FeatureFlagService, ResourceUtility, JobsStore
    ) {
        $scope.$state = $state;
        $scope.ResourceUtility = ResourceUtility;
        $scope.jobs = JobsStore.data.jobs;

        $scope.handleSidebarToggle = function ($event) {
            $("body").toggleClass("open-nav");
            $("body").addClass("controlled-nav");  // indicate the user toggled the nav
        }
        
        FeatureFlagService.GetAllFlags().then(function(result) {
            var flags = FeatureFlagService.Flags();
            $scope.showUserManagement = FeatureFlagService.FlagIsEnabled(flags.USER_MGMT_PAGE);
            $scope.showModelCreationHistory = FeatureFlagService.FlagIsEnabled(flags.MODEL_HISTORY_PAGE);
            $scope.showApiConsole = FeatureFlagService.FlagIsEnabled(flags.API_CONSOLE_PAGE);
            $scope.showMarketoSettings = FeatureFlagService.FlagIsEnabled(flags.USE_MARKETO_SETTINGS);
            $scope.showEloquaSettings = FeatureFlagService.FlagIsEnabled(flags.USE_ELOQUA_SETTINGS);
            $scope.showSalesforceSettings = FeatureFlagService.FlagIsEnabled(flags.USE_SALESFORCE_SETTINGS);
            $scope.showJobsPage = FeatureFlagService.FlagIsEnabled(flags.JOBS_PAGE);
            $scope.showLeadEnrichmentPage = FeatureFlagService.FlagIsEnabled(flags.LEAD_ENRICHMENT_PAGE);
        });

        $scope.statusFilter = function (item) { 
            return item.jobStatus === 'Running' || item.jobStatus === 'Pending'; 
        };
    });