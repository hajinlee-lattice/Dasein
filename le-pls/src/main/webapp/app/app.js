//Initial load of the application    
var mainApp = angular.module('mainApp', [
    'ui.bootstrap',
    'mainApp.appCommon.utilities.EvergageUtility',
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
    TimestampIntervalUtility, EvergageUtility, ResourceStringsService, HelpService, LoginService, ConfigService, SimpleModal) {
    $scope.showFooter = true;
    $scope.sessionExpired = false;
    
    var TIME_INTERVAL_BETWEEN_INACTIVITY_CHECKS = 30000;
    var TIME_INTERVAL_INACTIVITY_BEFORE_WARNING = 14.5 * 60 * 1000;  // 14.5 minutes
    var TIME_INTERVAL_WARNING_BEFORE_LOGOUT = 30000;
    
    var inactivityCheckingPromise = null;
    var warningModalInstance = null;
    
    var config = { attributes: true, childList: true, characterData: true, subtree: true };
    var observer = new MutationObserver(function(mutations) {
        /** This check is because we do not want the opening of the warning modal to cause session to refresh */
        if (!warningModalInstance) {
            refreshSessionLastActiveTimeStamp();
        }
    });
    
    ResourceStringsService.GetExternalResourceStringsForLocale().then(function(result) {
        var previousSession = BrowserStorageUtility.getClientSession();
        if (BrowserStorageUtility.getLoginDocument() && mustUserChangePassword(BrowserStorageUtility.getLoginDocument())) {
            $http.get('./app/core/views/MainView.html').success(function (html) {
                var scope = $rootScope.$new();
                scope.directToPassword = true;
                $compile($("#mainView").html(html))(scope);
            });
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
        startObservingDocumentBody();
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
                    
                    startObservingDocumentBody();
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
    
    $scope.privacyPolicyClick = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        HelpService.OpenPrivacyPolicy();
    };
    
    $scope.getWidgetConfigDoc = function () {
        ConfigService.GetWidgetConfigDocument().then(function(result) {
            $http.get('./app/core/views/MainView.html').success(function (html) {
                var scope = $rootScope.$new();
                $compile($("#mainView").html(html))(scope);
            });
        });
    };
    
    function mustUserChangePassword(loginDocument) {
        return loginDocument.MustChangePassword || TimestampIntervalUtility.isTimestampFartherThanNinetyDaysAgo(loginDocument.PasswordLastModified);
    }
    
    function refreshSessionLastActiveTimeStamp() {
        BrowserStorageUtility.setSessionLastActiveTimestamp(Date.now());
    }
    
    function startObservingDocumentBody() {
        observer.observe(document.body, config);
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
            refreshSessionLastActiveTimeStamp();
            startCheckingIfSessionIsInactive();
        };
    }
    
    function checkIfSessionIsInactiveEveryInterval() {
        if (Date.now() - BrowserStorageUtility.getSessionLastActiveTimestamp() >= TIME_INTERVAL_INACTIVITY_BEFORE_WARNING) {
            if (!warningModalInstance) {
                cancelCheckingIfSessionIsInactive();
                openWarningModal();
            }
            $timeout(callWhenWarningModalExpires, TIME_INTERVAL_WARNING_BEFORE_LOGOUT);
        }
    }

    function startCheckingIfSessionIsInactive() {
        inactivityCheckingPromise = $interval(checkIfSessionIsInactiveEveryInterval, TIME_INTERVAL_BETWEEN_INACTIVITY_CHECKS);
    }
    
    function cancelCheckingIfSessionIsInactive() {
        $interval.cancel(inactivityCheckingPromise);
    }

    function callWhenWarningModalExpires() {
        if (hasSessionTimedOut()) {
            $scope.sessionExpired = true;
            /** This line is actually necessary. Otherwise, user doesn't get properly logged out when tenant selection modal is up */
            hideTenantSelectionModal();
            LoginService.Logout();
        } else {
            if (warningModalInstance) {
                closeWarningModalAndSetInstanceToNull();
            }
            startCheckingIfSessionIsInactive();
        }
    }

    function hideTenantSelectionModal() {
        $("#modalContainer").modal('hide');
    }
    
    function closeWarningModalAndSetInstanceToNull() {
        warningModalInstance.close();
        warningModalInstance = null;
    }
});

mainApp.factory('authInterceptor', function ($rootScope, $q, $window, BrowserStorageUtility) {
  return {
    request: function (config) {
      config.headers = config.headers || {};
      if (BrowserStorageUtility.getTokenDocument()) {
        config.headers.Authorization = BrowserStorageUtility.getTokenDocument();
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