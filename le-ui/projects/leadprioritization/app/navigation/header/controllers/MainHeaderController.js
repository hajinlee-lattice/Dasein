angular.module('pd.navigation.header', [
    'mainApp.core.utilities.NavUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.modules.ServiceErrorModule',
    'mainApp.core.services.ResourceStringsService',
    'mainApp.core.services.FeatureFlagService',
    'mainApp.login.services.LoginService',
    'mainApp.login.controllers.UpdatePasswordController',
    'mainApp.models.controllers.ModelCreationHistoryController',
    'mainApp.models.controllers.ModelDetailController',
    'mainApp.appCommon.utilities.TimestampIntervalUtility',
    'mainApp.config.services.ConfigService',
    'common.utilities.SessionTimeout'
])
.controller('HeaderController', function (
    $scope, $rootScope, $state, ResourceUtility, BrowserStorageUtility, FeatureFlagService,
    LoginService, NavUtility, JobsStore
) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.jobs = JobsStore.data.jobs;

    $scope.statusFilter = function (item) {
        return item.jobStatus === 'Running' || item.jobStatus === 'Pending';
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

        $scope.mayChangePassword = (authenticationRoute !== 'SSO' || (authenticationRoute === 'SSO'  && accessLevel === 'SUPER_ADMIN'));

    $('body.not-initialized')
        .removeClass('not-initialized')
        .addClass('initialized');

    $rootScope.$on('$stateChangeSuccess', function(e, toState, toParams, fromState, fromParams) {
        if (toState.params) {
            setPageTitle(toState.params);
        }

        if (isModelDetailState(fromState.name) && ! isModelDetailState(toState.name)) {
            $scope.isModelDetailsPage = false;
        }
        
        if($scope.headerBack && toState.name && ($scope.headerBack.path && !toState.name.match($scope.headerBack.path))) {
            $scope.headerBack = null;
        }
    });

    function setPageTitle(params) {
        $scope.pageDisplayIcon = params.pageIcon ? params.pageIcon : null;
        $scope.pageDisplayName = params.pageTitle ? params.pageTitle : null;
    }

    setPageTitle($state.params);

    function isModelDetailState(stateName) {
        var stateNameArr = stateName.split('.');
        if (stateNameArr[0] == 'home' && stateNameArr[1] == 'model') {
            return true;
        }
        return false;
    }

    $scope.$on('model-details', function(event, args) {
        $scope.isModelDetailsPage = true;
        $scope.modelDisplayName = args.displayName;
    });

    $scope.$on('header-back', function(event, args) {
        /**
         * args.path is required.  It's a regex compared to toState.name. When it fails headerBack is destroyed, otherwise your header back stuff will persist.
         */
        if(args.path) {
            $scope.headerBack = args;
        }
    });
    
    checkBrowserWidth();
    var _checkBrowserWidth = _.debounce(checkBrowserWidth, 250);

    angular.element(window).resize(_checkBrowserWidth);

    $scope.handleSidebarToggle = function ($event) {
        angular.element("body").toggleClass("open-nav");
        angular.element("body").addClass("controlled-nav");  // indicate the user toggled the nav
    }

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
        if (data != null && data.Success) {
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
