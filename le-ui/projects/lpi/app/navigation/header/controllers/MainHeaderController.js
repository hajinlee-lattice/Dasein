angular.module('lp.navigation.header', [
    'mainApp.core.utilities.NavUtility',
    'common.utilities.browserstorage',
    'common.exceptions',
    'mainApp.core.services.ResourceStringsService',
    'common.services.featureflag',
    'mainApp.login.services.LoginService',
    'mainApp.login.controllers.UpdatePasswordController',
    'mainApp.models.controllers.ModelCreationHistoryController',
    'mainApp.models.controllers.ModelDetailController',
    'mainApp.appCommon.utilities.TimestampIntervalUtility',
    'mainApp.config.services.ConfigService',
    'common.utilities.SessionTimeout',
    'common.navigation.back'
])
.controller('HeaderController', function (
    $scope, $rootScope, $state, $transitions, ResourceUtility, NavUtility, 
    BrowserStorageUtility, FeatureFlagService, LoginService, JobsStore, ApiHost
) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.jobs = JobsStore.data.jobs;
    $scope.importJobs = JobsStore.data.importJobs;
    $scope.exportJobs = JobsStore.data.exportJobs;

    $transitions.onStart({}, function(trans) {
        var to = trans.$to(),
            params = trans.params('to'),
            from = trans.$from();

        if (params.pageIcon) {
            setPageTitle(params);
        }
    });


    /**
     * It returns the state to go to for the jobs
     * If CDL the firts tab is going to be P&A jobs
     */
    $scope.getJobSRef = function(){
        var state = '';
        var flags = FeatureFlagService.Flags();
        var cdl = FeatureFlagService.FlagIsEnabled(flags.ENABLE_CDL);
        
        if (cdl === true){
            state = 'home.jobs.data';
        } else {
            state = 'home.jobs.status';
        }

        return state;
    };

    $scope.getActiveJobs = function() {
        return JobsStore.data.allActiveJobs;
    };

    $scope.statusFilter = function (item) {
        return item.jobStatus === 'Running' || item.jobStatus === 'Pending';
    };

    $scope.statusImportFilter = function(item){
        return item.jobStatus === 'Running' || item.jobStatus === 'Pending';
    };

    $scope.getRunningSubJobsDataCount = function(){
        return JobsStore.data.subjobsRunning.length;
    };


    FeatureFlagService.GetAllFlags().then(function(result) {
        var flags = FeatureFlagService.Flags();
        $scope.showUserManagement = FeatureFlagService.FlagIsEnabled(flags.USER_MGMT_PAGE);
        $scope.showJobsPage = FeatureFlagService.FlagIsEnabled(flags.JOBS_PAGE);
    });

    var ClientSession = BrowserStorageUtility.getClientSession();

    if (ClientSession != null) {
        var LoginDocument = BrowserStorageUtility.getLoginDocument();
        var Tenants = LoginDocument ? LoginDocument.Tenants : {};
        var Tenant = ClientSession ? ClientSession.Tenant : {};

        $scope.userDisplayName = ClientSession.DisplayName;
        $scope.tenantName = window.escape(Tenant.DisplayName);
        $scope.tenants = Tenants;
    }

    $scope.showProfileNav = false;

    var loginDocument = BrowserStorageUtility.getLoginDocument(),
        authenticationRoute = loginDocument.AuthenticationRoute || null,
        clientSession = BrowserStorageUtility.getClientSession() || {},
        accessLevel = clientSession.AccessLevel || null;

    $scope.mayChangePassword = authenticationRoute !== 'SSO';

    $scope.mayViewSSOConfig = typeof BrowserStorageUtility.getClientSession().AvailableRights.PLS_SSO_Config != 'undefined' &&
                                BrowserStorageUtility.getClientSession().AvailableRights.PLS_SSO_Config.MayView === true;

    $('body.not-initialized')
        .removeClass('not-initialized')
        .addClass('initialized');

    function setPageTitle(params) {
        $scope.pageDisplayIcon = params.pageIcon ? params.pageIcon : null;
        $scope.pageDisplayName = params.pageTitle ? params.pageTitle : null;
    }

    setPageTitle($state.params);

    checkBrowserWidth();
    var _checkBrowserWidth = _.debounce(checkBrowserWidth, 250);

    angular.element(window).resize(_checkBrowserWidth);

    $scope.handleSidebarToggle = function ($event) {
        angular.element("body").toggleClass("open-nav");
        angular.element("body").addClass("controlled-nav");  // indicate the user toggled the nav
    };

    $(document.body).click(function() {
        if ($scope.showProfileNav) {
            $scope.showProfileNav = false;
            $scope.$apply();
        }
    });

    $scope.headerClicked = function($event) {
        $scope.showProfileNav = !$scope.showProfileNav;
        $event.stopPropagation();
    };

    function checkBrowserWidth(){
        // if the user has closed the nav, leave it closed when increasing size
        if (window.matchMedia("(min-width: 1200px)").matches && !angular.element("body").hasClass("controlled-nav")) {
            if (typeof(sessionStorage) !== 'undefined') {
                if(sessionStorage.getItem('open-nav') === 'true') {
                    angular.element("body").addClass('open-nav');
                } else {
                    angular.element("body").removeClass('open-nav');
                }
            }
        } else {
            if(angular.element("body").hasClass("open-nav")) {
                // if the nav is open when scaling down close it but allow it to re-open by removing our user controlled class indicator
                angular.element("body").removeClass("controlled-nav");
            }
            angular.element("body").removeClass("open-nav");
        }
    }

    // Handle when the Update Password link is clicked
    $scope.$on(NavUtility.UPDATE_PASSWORD_NAV_EVENT, function (event, data) {
        if (data !== null && data.Success) {
            createUpdatePasswordSuccessView();
        } else {
            $state.go('home.updatepassword');
        }
    });

    function createUpdatePasswordSuccessView() {
        $('#mainHeaderView').hide();
        //LoginService.Logout();
        $state.go('passwordsuccess');
    }
});
