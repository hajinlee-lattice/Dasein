angular
    .module('pd.navigation.sidebar', [
        'mainApp.appCommon.utilities.ResourceUtility',
        'mainApp.appCommon.utilities.StringUtility',
        'mainApp.core.services.FeatureFlagService'
    ])
    .controller('SidebarRootController', function(
        $scope, $rootScope, $state, FeatureFlagService, ResourceUtility, JobsStore, ModelRatingsService
    ) {
        $scope.$state = $state;
        $scope.ResourceUtility = ResourceUtility;

        $scope.handleSidebarToggle = function ($event) {
            var target = angular.element($event.target),
                collapsable_click = !target.parents('.menu').length;
            if(collapsable_click) {
                $('body').toggleClass('open-nav');
                $('body').addClass('controlled-nav');  // indicate the user toggled the nav

                if (typeof(sessionStorage) !== 'undefined'){
                    sessionStorage.setItem('open-nav', $('body').hasClass('open-nav'));
                }
                $rootScope.$broadcast('sidebar:toggle');
            }
        }

        FeatureFlagService.GetAllFlags().then(function(result) {
            var flags = FeatureFlagService.Flags();
            $scope.showUserManagement = FeatureFlagService.FlagIsEnabled(flags.USER_MGMT_PAGE);
            $scope.showModelCreationHistory = FeatureFlagService.FlagIsEnabled(flags.MODEL_HISTORY_PAGE);
            $scope.showApiConsole = FeatureFlagService.FlagIsEnabled(flags.API_CONSOLE_PAGE);
            $scope.showMarketoSettings = FeatureFlagService.FlagIsEnabled(flags.USE_MARKETO_SETTINGS);
            $scope.showEloquaSettings = FeatureFlagService.FlagIsEnabled(flags.USE_ELOQUA_SETTINGS);
            $scope.showSalesforceSettings = FeatureFlagService.FlagIsEnabled(flags.USE_SALESFORCE_SETTINGS);
            $scope.showCampaignsPage = FeatureFlagService.FlagIsEnabled(flags.CAMPAIGNS_PAGE);
            $scope.showAnalysisPage = FeatureFlagService.FlagIsEnabled(flags.ENABLE_CDL);
            $scope.showPlayBook = FeatureFlagService.FlagIsEnabled(flags.ENABLE_CDL);
            $scope.showRatingsEngine = true;//FeatureFlagService.FlagIsEnabled(flags.ENABLE_CDL);
            $scope.showSegmentationPage = FeatureFlagService.FlagIsEnabled(flags.ENABLE_CDL);
            $scope.showLatticeInsightsPage = FeatureFlagService.FlagIsEnabled(flags.LATTICE_INSIGHTS);
            $scope.showContactUs = false;
        });

        angular.extend($scope, {
            init: function(){
                if (typeof(sessionStorage) !== 'undefined') {
                    if(sessionStorage.getItem('open-nav') === 'true' || !sessionStorage.getItem('open-nav')) {
                        $("body").addClass('open-nav');
                    } else {
                        $("body").removeClass('open-nav');
                    }
                }
            }
        })

        $scope.init();

    });