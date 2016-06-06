//Initial load of the application
var mainApp = angular.module('mainApp', [
    'templates-main',
    'ui.router',
    'ui.bootstrap',
    'oc.lazyLoad',
    'mainApp.appCommon.utilities.EvergageUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.TimestampIntervalUtility',
    'mainApp.core.modules.ServiceErrorModule',
    'mainApp.core.controllers.MainViewController',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.services.ResourceStringsService',
    'mainApp.core.services.HelpService',
    'mainApp.core.services.FeatureFlagService',
    'mainApp.login.controllers.LoginController',
    'mainApp.login.services.LoginService',
    'mainApp.config.services.ConfigService',
    'mainApp.create.csvImport',
    'mainApp.create.csvReport',
    'mainApp.create.csvBulkUpload',
    'mainApp.sfdc.sfdcCredentials',
    'mainApp.create.controller.ImportJobController',
    'pd.navigation',
    'pd.jobs',
    'pd.apiconsole'
])

.config(['$httpProvider', function($httpProvider) {
    /*
    //initialize get if not there
    if (!$httpProvider.defaults.headers.get) {
        $httpProvider.defaults.headers.get = {};    
    }
    // disable IE ajax request caching
    $httpProvider.defaults.headers.get['If-Modified-Since'] = 'Mon, 26 Jul 1997 05:00:00 GMT';
    // extra
    $httpProvider.defaults.headers.get['Cache-Control'] = 'no-cache';
    $httpProvider.defaults.headers.get['Pragma'] = 'no-cache';
    */
}])

// {{ foobar | escape }}
.filter('escape', function() {
  return window.escape;
})

.controller('MainController', function ($scope, $templateCache, $http, $rootScope, $compile, $interval, $modal, $timeout, BrowserStorageUtility, ResourceUtility,
    TimestampIntervalUtility, EvergageUtility, ResourceStringsService, HelpService, LoginService, ConfigService) {
    $scope.showFooter = true;
    $scope.sessionExpired = false;

    var TIME_INTERVAL_BETWEEN_INACTIVITY_CHECKS = 30 * 1000;
    var TIME_INTERVAL_INACTIVITY_BEFORE_WARNING = 14.5 * 60 * 1000;  // 14.5 minutes
    var TIME_INTERVAL_WARNING_BEFORE_LOGOUT = 30 * 1000;

    var inactivityCheckingId = null;
    var warningModalInstance = null;

    var previousSession = BrowserStorageUtility.getClientSession();
    var loginDocument = BrowserStorageUtility.getLoginDocument();

    setTimeout(function() {
        if (loginDocument && mustUserChangePassword(loginDocument)) {
            window.open("/login", "_self");
        } else if (previousSession != null && !hasSessionTimedOut()) {
            $scope.refreshPreviousSession(previousSession.Tenant);
        } else {
            window.open("/login", "_self");
        }
    },0)

    $rootScope.$on('$stateChangeStart', function(ev, toState, toParams, fromState, fromParams) {
        if (fromState.name == 'home.models' && toState.name == 'home') {
            ev.preventDefault();
            window.open("/login", "_self");
        }
    });

    $scope.$on("LoggedIn", function() {
        startObservingUserActivtyThroughMouseAndKeyboard();
        startCheckingIfSessionIsInactive();
    });

    $scope.refreshPreviousSession = function (tenant) {
        //Refresh session and go somewhere
        LoginService.GetSessionDocument(tenant).then(
            // Success
            function (data, status) {
                if (data && data.Success === true) {
                    //Initialize Evergage
                    EvergageUtility.Initialize({
                        userID: data.Result.User.Identifier,
                        title: data.Result.User.Title,
                        datasetPrefix: "pls",
                        company: data.Ticket.Tenants[0].DisplayName
                    });

                    $scope.getLocaleSpecificResourceStrings(data.Result.User.Locale);

                    startObservingUserActivtyThroughMouseAndKeyboard();
                    startCheckingIfSessionIsInactive();
                }
            },

            // Fail
            function (data, status) {

            }
        );
    };

    // Handle when the copyright footer should be shown
    $scope.$on("ShowFooterEvent", function (event, data) {
        $scope.showFooter = data;
        if ($scope.showFooter === true) {
            $scope.copyrightString = ResourceUtility.getString('FOOTER_COPYRIGHT', [(new Date()).getFullYear()]);
            $scope.privacyPolicyString = ResourceUtility.getString('HEADER_PRIVACY_POLICY');
        }
    });
    
    $scope.getLocaleSpecificResourceStrings = function (locale) {
        /*
        ResourceStringsService.GetInternalResourceStringsForLocale(locale).then(function(result) {
            $scope.copyrightString = ResourceUtility.getString('FOOTER_COPYRIGHT', [(new Date()).getFullYear()]);
            $scope.privacyPolicyString = ResourceUtility.getString('HEADER_PRIVACY_POLICY');
        });
        $scope.getWidgetConfigDoc();
        */
    };

    $scope.getWidgetConfigDoc = function () {
        /*
        ConfigService.GetWidgetConfigDocument().then(function(result) {
            $http.get('app/core/views/MainView.html', { cache: $templateCache }).success(function (html) {
                var scope = $rootScope.$new();
                $compile($("#mainView").html(html))(scope);
            });
        });
        */
    };

    function startObservingUserActivtyThroughMouseAndKeyboard() {
        $(this).mousemove(function (e) {
            if (!warningModalInstance) {
                refreshSessionLastActiveTimeStamp();
            }
        });
        $(this).keypress(function (e) {
            if (!warningModalInstance) {
                refreshSessionLastActiveTimeStamp();
            }
        });
    }

    function startCheckingIfSessionIsInactive() {
        refreshSessionLastActiveTimeStamp();
        inactivityCheckingId = setInterval(checkIfSessionIsInactiveEveryInterval, TIME_INTERVAL_BETWEEN_INACTIVITY_CHECKS); // 1 minute
    }

    function checkIfSessionIsInactiveEveryInterval() {
        if (Date.now() - BrowserStorageUtility.getSessionLastActiveTimestamp() >= TIME_INTERVAL_INACTIVITY_BEFORE_WARNING) {
            if (!warningModalInstance) {
                cancelCheckingIfSessionIsInactiveAndSetIdToNull();
                openWarningModal();
            }
            $timeout(callWhenWarningModalExpires, TIME_INTERVAL_WARNING_BEFORE_LOGOUT);
        }
    }
    
    function mustUserChangePassword(loginDocument) {
        return loginDocument.MustChangePassword || TimestampIntervalUtility.isTimestampFartherThanNinetyDaysAgo(loginDocument.PasswordLastModified);
    }
    
    function refreshSessionLastActiveTimeStamp() {
        BrowserStorageUtility.setSessionLastActiveTimestamp(Date.now());
    }
    
    function hasSessionTimedOut() {
        return Date.now() - BrowserStorageUtility.getSessionLastActiveTimestamp() >=
            TIME_INTERVAL_INACTIVITY_BEFORE_WARNING + TIME_INTERVAL_WARNING_BEFORE_LOGOUT;
    }

    function openWarningModal() {
        warningModalInstance = $modal.open({
            animation: true,
            backdrop: false,
            scope: $scope,
            templateUrl: 'app/core/views/WarningModal.html'
        });

        $scope.refreshSession = function() {
            closeWarningModalAndSetInstanceToNull();
            startCheckingIfSessionIsInactive();
        };
    }
    
    function cancelCheckingIfSessionIsInactiveAndSetIdToNull() {
        clearInterval(inactivityCheckingId);
        inactivityCheckingId = null;
    }

    function stopObservingUserInteractionBasedOnMouseAndKeyboard() {
        $(this).off("mousemove");
        $(this).off("keypress");
    }
    
    function callWhenWarningModalExpires() {
        if (hasSessionTimedOut()) {
            $scope.sessionExpired = true;
            /** This line is actually necessary. Otherwise, user doesn't get properly logged out when tenant selection modal is up */
            hideTenantSelectionModal();
            stopObservingUserInteractionBasedOnMouseAndKeyboard();
            LoginService.Logout();
        } else {
            if (warningModalInstance) {
                closeWarningModalAndSetInstanceToNull();
            }
            if (!inactivityCheckingId) {
                startCheckingIfSessionIsInactive();
            }
        }
    }

    function hideTenantSelectionModal() {
        $("#modalContainer").modal('hide');
    }

    function closeWarningModalAndSetInstanceToNull() {
        warningModalInstance.close();
        warningModalInstance = null;
    }

    function createMandatoryChangePasswordViewForLocale(locale) {
        ResourceStringsService.GetInternalResourceStringsForLocale(locale).then(function(result) {
            $http.get('app/core/views/MainView.html', { cache: $templateCache }).success(function (html) {
                var scope = $rootScope.$new();
                scope.isLoggedInWithTempPassword = $scope.isLoggedInWithTempPassword;
                scope.isPasswordOlderThanNinetyDays = $scope.isPasswordOlderThanNinetyDays;
                $compile($("#mainView").html(html))(scope);
            });
        });
    }
});

mainApp.factory('authInterceptor', function ($rootScope, $q, BrowserStorageUtility) {
    return {
        request: function(config) {
            config.headers = config.headers || {};
            
            if (config.headers.Authorization == null && BrowserStorageUtility.getTokenDocument()) {
                config.headers.Authorization = BrowserStorageUtility.getTokenDocument();
            }
            
            return config;
        },
        response: function(response) {
            return response || $q.when(response);
        }
    };
});

mainApp.config(function ($httpProvider) {
    $httpProvider.interceptors.push('authInterceptor');
});