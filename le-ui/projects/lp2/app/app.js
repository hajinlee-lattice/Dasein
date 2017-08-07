//Initial load of the application    
var mainApp = angular.module('mainApp', [
    'ui.bootstrap',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.TimestampIntervalUtility',
    'mainApp.core.services.ResourceStringsService',
    'mainApp.core.services.HelpService',
    'mainApp.login.services.LoginService',
    'mainApp.config.services.ConfigService',
    'mainApp.login.controllers.LoginController',
    'mainApp.core.controllers.MainViewController',
    'mainApp.appCommon.modals.SimpleModal'
])

.config(['$httpProvider', function($httpProvider) {
    //initialize get if not there
    if (!$httpProvider.defaults.headers.get) {
        $httpProvider.defaults.headers.get = {};    
    }

    // disable IE ajax request caching
    $httpProvider.defaults.headers.get['If-Modified-Since'] = 'Mon, 26 Jul 1997 05:00:00 GMT';
    // extra
    $httpProvider.defaults.headers.get['Cache-Control'] = 'no-cache';
    $httpProvider.defaults.headers.get['Pragma'] = 'no-cache';
}])

.controller('MainController', function ($scope, $http, $rootScope, $compile, $interval, $modal, $timeout, BrowserStorageUtility, ResourceUtility,
    TimestampIntervalUtility, ResourceStringsService, HelpService, LoginService, ConfigService, SimpleModal) {
    $scope.showFooter = true;
    $scope.sessionExpired = false;
    
    var TIME_INTERVAL_BETWEEN_INACTIVITY_CHECKS = 30 * 1000;
    var TIME_INTERVAL_INACTIVITY_BEFORE_WARNING = 14.5 * 60 * 1000;  // 14.5 minutes
    var TIME_INTERVAL_WARNING_BEFORE_LOGOUT = 30 * 1000;
    
    var inactivityCheckingId = null;
    var warningModalInstance = null;

    ResourceStringsService.GetExternalResourceStringsForLocale().then(function(result) {
        var previousSession = BrowserStorageUtility.getClientSession();
        var loginDocument = BrowserStorageUtility.getLoginDocument();
        if (loginDocument && mustUserChangePassword(loginDocument)) {
            $scope.isLoggedInWithTempPassword = loginDocument.MustChangePassword;
            $scope.isPasswordOlderThanNinetyDays = TimestampIntervalUtility.isTimestampFartherThanNinetyDaysAgo(loginDocument.PasswordLastModified);
            createMandatoryChangePasswordViewForLocale(previousSession.Locale);
        } else if (previousSession != null && ! hasSessionTimedOut()) {
            $scope.refreshPreviousSession(previousSession.Tenant);
        } else {
            $scope.showFooter = false;
            // Create the Login View
            $http.get('./app/login/views/LoginView.html').success(function (html) {
                var scope = $rootScope.$new();
                $compile($("#mainView").html(html))(scope);
            });
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
        ResourceStringsService.GetInternalResourceStringsForLocale(locale).then(function(result) {
            $scope.copyrightString = ResourceUtility.getString('FOOTER_COPYRIGHT', [(new Date()).getFullYear()]);
            $scope.privacyPolicyString = ResourceUtility.getString('HEADER_PRIVACY_POLICY');
            $scope.getWidgetConfigDoc();
        });
    };

    $scope.getWidgetConfigDoc = function () {
        ConfigService.GetWidgetConfigDocument().then(function(result) {
            $http.get('./app/core/views/MainView.html').success(function (html) {
                var scope = $rootScope.$new();
                $compile($("#mainView").html(html))(scope);
            });
        });
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
            $http.get('./app/core/views/MainView.html').success(function (html) {
                var scope = $rootScope.$new();
                scope.isLoggedInWithTempPassword = $scope.isLoggedInWithTempPassword;
                scope.isPasswordOlderThanNinetyDays = $scope.isPasswordOlderThanNinetyDays;
                $compile($("#mainView").html(html))(scope);
            });
        });
    }
});

mainApp.factory('authInterceptor', function ($rootScope, $q, $window, BrowserStorageUtility) {
  return {
    request: function (config) {
      config.headers = config.headers || {};
      if (BrowserStorageUtility.getTokenDocument()) {
        config.headers.Authorization = BrowserStorageUtility.getTokenDocument();

        var ClientSession = BrowserStorageUtility.getClientSession();

        if (ClientSession && ClientSession.Tenant) {
            config.headers.TenantId = ClientSession.Tenant.Identifier;
        }
      }
      return config;
    },
    response: function (response) {
      if (response.status === 401) {
        // handle the case where the user is not authenticated
      }
      return response || $q.when(response);
    }
  };
});

mainApp.config(function ($httpProvider) {
  $httpProvider.interceptors.push('authInterceptor');
});
