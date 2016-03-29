angular.module('mainApp.core.controllers.MainViewController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.MetadataUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.login.services.LoginService',
    'mainApp.config.services.ConfigService',
    'mainApp.core.controllers.MainHeaderController',
    'mainApp.config.controllers.ManageCredentialsController',
    'mainApp.login.controllers.UpdatePasswordController',
    'mainApp.userManagement.controllers.UserManagementController',
    'mainApp.models.controllers.AdminInfoController',
    'mainApp.models.controllers.ModelListController',
    'mainApp.models.controllers.ModelDetailController',
    'mainApp.models.controllers.ModelCreationHistoryController',
    'mainApp.models.controllers.ActivateModelController',
    'mainApp.core.services.FeatureFlagService',
    'mainApp.setup.controllers.SetupController',
    'mainApp.setup.controllers.DeploymentWizardController',
    'mainApp.setup.controllers.LeadEnrichmentController'
])

.controller('MainViewController', function ($scope, $templateCache, $http, $rootScope, $state, $compile, ResourceUtility, BrowserStorageUtility, TimestampIntervalUtility, LoginService, NavUtility, FeatureFlagService, ConfigService) {
    $scope.ResourceUtility = ResourceUtility;

    // Handle Initial View
    $http.get('app/core/views/MainHeaderView.html', { cache: $templateCache }).success(function (html) {
        var scope = $rootScope.$new();
        scope.mandatePasswordChange = $scope.isLoggedInWithTempPassword || $scope.isPasswordOlderThanNinetyDays;
        $compile($("#mainHeaderView").html(html))(scope);
    });

    //createMainContentViewAndRefreshFeatures();

    // Handle when the Update Password link is clicked
    $scope.$on(NavUtility.UPDATE_PASSWORD_NAV_EVENT, function (event, data) {
        if (data != null && data.Success) {
            createUpdatePasswordSuccessView(); console.log('buh',data);
        } else {
            $state.go('home.updatepassword');
        }
    });

    function createUpdatePasswordSuccessView() {
        $('#mainHeaderView').hide();
        LoginService.Logout();
        $state.go('passwordsuccess');
    }

    // Handle when the User Management link is clicked
    $scope.$on(NavUtility.USER_MANAGEMENT_NAV_EVENT, function (event, data) {
        createUserManagementView();
    });

    function createUserManagementView() {
        // Set the hash
        //window.location.hash = NavUtility.USER_MANAGEMENT_HASH;

        // Fetch the view and make it Angular aware
        $state.go('users', {}, { reload: true } );
    }
    /*
    function createMainContentViewAndRefreshFeatures() {
        FeatureFlagService.GetAllFlags().then(function() {
            var flags = FeatureFlagService.Flags();

            // Redirect to DeploymentWizard functionality disabled for LP3 M1

            //if (FeatureFlagService.FlagIsEnabled(flags.REDIRECT_TO_DEPLOYMENT_WIZARD_PAGE)) {
            //    $state.go('deploymentwizard');
                //createDeploymentWizardView();
            //}
        });
    }

    // Handle when the Manage Credentials link is clicked
    $scope.$on(NavUtility.MANAGE_CREDENTIALS_NAV_EVENT, function (event, data) {
        createManageCredentialsView();
    });

    function createManageCredentialsView() {
        // Set the hash
        //window.location.hash = NavUtility.MANAGE_CREDENTIALS_HASH;

        // Fetch the view and make it Angular aware
        $http.get('./app/config/views/ManageCredentialsView.html').success(function (html) {
            var scope = $rootScope.$new();
            $compile($("#mainContentView").html(html))(scope);
        });
    }

    // Handle when the User Management link is clicked
    $scope.$on(NavUtility.USER_MANAGEMENT_NAV_EVENT, function (event, data) {
        createUserManagementView();
    });

    function createUserManagementView() {
        // Set the hash
        //window.location.hash = NavUtility.USER_MANAGEMENT_HASH;

        // Fetch the view and make it Angular aware
        $http.get('./app/userManagement/views/UserManagementView.html').success(function (html) {
            var scope = $rootScope.$new();
            $compile($("#mainContentView").html(html))(scope);
        });
    }

    // Handle when the User Management link is clicked
    $scope.$on(NavUtility.ADMIN_INFO_NAV_EVENT, function (event, data) {
        createAdminInfoView(data);
    });

    function createAdminInfoView(data) {
        // Set the hash
        //window.location.hash = NavUtility.ADMIN_INFO_HASH;

        // Fetch the view and make it Angular aware
        $http.get('./app/models/views/AdminInfoView.html').success(function (html) {
            var scope = $rootScope.$new();
            scope.data = data;
            $compile($("#mainContentView").html(html))(scope);
        });
    }

    $scope.$on(NavUtility.MODEL_CREATION_HISTORY_NAV_EVENT, function (event, data) {
        modelCreationHistoryView();
    });

    function modelCreationHistoryView() {
        //window.location.hash = NavUtility.MODEL_CREATION_HISTORY_HASH;

        $http.get('./app/models/views/ModelCreationHistoryView.html').success(function (html) {
            var scope = $rootScope.$new();
            $compile($("#mainContentView").html(html))(scope);
        });
    }

    // Handle when the Model List link is clicked
    $scope.$on(NavUtility.MODEL_LIST_NAV_EVENT, function (event, data) {
        //createModelListView();
    });

    function createModelListView() {
        // Set the hash
        //window.location.hash = NavUtility.MODEL_LIST_HASH;

        // Fetch the view and make it Angular aware
        $http.get('./app/models/views/ModelListView.html').success(function (html) {
            var scope = $rootScope.$new();
            $compile($("#mainContentView").html(html))(scope);
        });
    }

    // Handle when the Model Detail link is clicked
    $scope.$on(NavUtility.MODEL_DETAIL_NAV_EVENT, function (event, data) {
        //createModelDetailView(data);
    });

    function createModelDetailView(data) {
        return;
        // Set the hash
        //window.location.hash = NavUtility.MODEL_DETAIL_HASH;

        // Fetch the view and make it Angular aware
        $http.get('./app/models/views/ModelDetailView.html').success(function (html) {
            var scope = $rootScope.$new();
            scope.data = data;
            $compile($("#mainContentView").html(html))(scope);
        });
    }

    // Handle when the Update Password link is clicked
    $scope.$on(NavUtility.ACTIVATE_MODEL, function (event, data) {
        //createActivateModelView();
    });

    function createActivateModelView() {
        return;
        // Set the hash
        //window.location.hash = NavUtility.ACTIVATE_MODEL;

        // Fetch the view and make it Angular aware
        $http.get('./app/models/views/ActivateModelView.html').success(function (html) {
            var scope = $rootScope.$new();
            $compile($("#mainContentView").html(html))(scope);
        });
    }

    // Handle when the Setup link is clicked
    $scope.$on(NavUtility.SETUP_NAV_EVENT, function (event, data) {
        //createSetupView();
    });

    function createSetupView() {
        // Set the hash
        //window.location.hash = NavUtility.SETUP_HASH;

        // Fetch the view and make it Angular aware
        $http.get('./app/setup/views/SetupView.html').success(function (html) {
            var scope = $rootScope.$new();
            $compile($("#mainContentView").html(html))(scope);
        });
    }

    // Handle the deployment wizard nav event
    $scope.$on(NavUtility.DEPLOYMENT_WIZARD_NAV_EVENT, function (event, data) {
        createDeploymentWizardView();
    });

    function createDeploymentWizardView() {
        // Set the hash
        //window.location.hash = NavUtility.DEPLOYMENT_WIZARD_HASH;

        // Fetch the view and make it Angular aware
        $http.get('./app/setup/views/DeploymentWizardView.html').success(function (html) {
            var scope = $rootScope.$new();
            $compile($("#mainContentView").html(html))(scope);
        });
    }

    // Handle the lead enrichment nav event
    $scope.$on(NavUtility.LEAD_ENRICHMENT_NAV_EVENT, function (event, data) {
        //createLeadEnrichmentView();
    });

    function createLeadEnrichmentView() {
        // Set the hash
        //window.location.hash = NavUtility.DEPLOYMENT_WIZARD_HASH;

        // Fetch the view and make it Angular aware
        $http.get('./app/setup/views/LeadEnrichmentView.html').success(function (html) {
            var scope = $rootScope.$new();
            $compile($("#mainContentView").html(html))(scope);
        });
    }
    */
});