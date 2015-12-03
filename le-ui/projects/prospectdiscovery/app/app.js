//Initial load of the application    
var mainApp = angular.module('mainApp', [
    'ui.router',
    'ui.bootstrap',
    'mainApp.appCommon.utilities.EvergageUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.TimestampIntervalUtility',
    'mainApp.core.services.ResourceStringsService',
    'mainApp.core.services.HelpService',
    'mainApp.core.controllers.MainHeaderController',
    'mainApp.core.controllers.MainViewController',
    'mainApp.login.services.LoginService',
    'mainApp.config.services.ConfigService',
    'mainApp.appCommon.modals.SimpleModal',
    'controllers.markets.builder',
    'controllers.navigation.table',
    'controllers.navigation.message',
    'controllers.navigation.links',
    'controllers.navigation.navdash',
    'controllers.navigation.subnav'
])

.run(['$rootScope', '$state', function($rootScope, $state) {
    $rootScope.$on('$stateChangeStart', function(evt, to, params) {
      if (to.redirectTo) {
        evt.preventDefault();
        $state.go(to.redirectTo, params)
      }
    });
}])

.config(['$stateProvider', '$urlRouterProvider', function($stateProvider, $urlRouterProvider) {
    $urlRouterProvider.otherwise('/');

    $stateProvider
        .state('home', {
            url: '/',
            //templateUrl: './app/main/MainView.html'
            redirectTo: 'markets.dashboard'
        })
        .state('fingerprints', {
            url: '/fingerprints',
            templateUrl: './app/fingerprints/FingerprintsView.html'
        })
        .state('markets', {
            url: '/markets',
            redirectTo: 'markets.dashboard',
            template:
                '<div ui-view="navigation"></div>' +
                '<div ui-view="summary"></div>' +
                '<div ui-view="main"></div>'
        })
        .state('markets.dashboard', {
            url: '/dashboard',
            views: {
                "navigation": {
                    templateUrl: './app/navigation/links/LinksView.html'
                },
                "summary": {
                    templateUrl: './app/navigation/navdash/NavDashView.html'
                },
                "main": {
                    templateUrl: './app/markets/dashboard/DashboardView.html'
                }
            }
        })
        .state('markets.list', {
            url: '/status',
            views: {
                "navigation": {
                    templateUrl: './app/navigation/links/LinksView.html'
                },
                "summary": {
                    template: ''
                },
                "main": {
                    templateUrl: './app/markets/MarketsView.html'
                }
            }
        })
        .state('markets.builder', {
            url: '/builder',
            views: {
                "navigation": {
                    templateUrl: './app/navigation/links/LinksView.html'
                },
                "summary": {
                    templateUrl: './app/navigation/subnav/SubNavView.html'
                },
                "main": {
                    templateUrl: './app/markets/builder/BuilderView.html'
                }
            }
        })
        .state('markets.prospect_schedule', {
            url: '/prospect_schedule',
            views: {
                "navigation": {
                    templateUrl: './app/navigation/links/LinksView.html'
                },
                "summary": {
                    template: ''
                },
                "main": {
                    templateUrl: './app/markets/prospect/ScheduleView.html'
                }
            }
        })
        .state('markets.prospect_list', {
            url: '/prospect_list',
            views: {
                "navigation": {
                    templateUrl: './app/navigation/links/LinksView.html'
                },
                "summary": {
                    template: ''
                },
                "main": {
                    templateUrl: './app/markets/prospect/ListView.html'
                }
            }
        })
        .state('jobs', {
            url: '/jobs',
            template:
                '<div ui-view="summary"></div>' +
                '<div ui-view="main"></div>'
        })
        .state('jobs.status', {
            url: '/status',
            views: {
                "summary@jobs": {
                    templateUrl: './app/navigation/table/TableView.html'
                },
                "main@jobs": {
                    templateUrl: './app/jobs/status/StatusView.html'
                }
            }
        })
        .state('jobs.import', {
            url: '/import'
        })
        .state('jobs.import.credentials', {
            url: '/credentials',
            views: {
                "summary@jobs": {
                    templateUrl: './app/navigation/message/MessageView.html'
                },
                "main@jobs": {
                    templateUrl: './app/jobs/import/credentials/CredentialsView.html'
                }
            }
        })
        .state('jobs.import.file', {
            url: '/file',
            views: {
                "summary@jobs": {
                    templateUrl: './app/navigation/message/MessageView.html'
                },
                "main@jobs": {
                    templateUrl: './app/jobs/import/file/FileView.html'
                }
            }
        })
        .state('jobs.import.processing', {
            url: '/processing',
            views: {
                "summary@jobs": {
                    templateUrl: './app/navigation/message/MessageView.html'
                },
                "main@jobs": {
                    templateUrl: './app/jobs/import/processing/ProcessingView.html'
                }
            }
        })
        .state('jobs.import.ready', {
            url: '/ready/:jobId',
            views: {
                "summary@jobs": {
                    templateUrl: './app/navigation/table/TableView.html'
                },
                "main@jobs": {
                    templateUrl: './app/jobs/import/ready/ReadyView.html'
                }
            }
        })
        .state('admin', {
            url: '/admin',
            templateUrl: './app/admin/AdminView.html'
        });
}])

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
    $scope.showFooter = false;
    $scope.sessionExpired = false;
    
    var TIME_INTERVAL_BETWEEN_INACTIVITY_CHECKS = 30 * 1000;
    var TIME_INTERVAL_INACTIVITY_BEFORE_WARNING = 14.5 * 60 * 1000;  // 14.5 minutes
    var TIME_INTERVAL_WARNING_BEFORE_LOGOUT = 30 * 1000;
    
    var inactivityCheckingId = null;
    var warningModalInstance = null;

    ResourceStringsService.GetExternalResourceStringsForLocale().then(function(result) {
        var previousSession = BrowserStorageUtility.getClientSession();
        var loginDocument = BrowserStorageUtility.getLoginDocument();

        console.log('init',hasSessionTimedOut(),previousSession);

        if (previousSession != null && !hasSessionTimedOut()) {
            $http.get('./app/header/HeaderView.html').success(function (html) {
                var scope = $rootScope.$new();
                $compile($("#mainHeaderView").html(html))(scope);
            });
            /*
            $http.get('./app/subnav/SubNavView.html').success(function (html) {
                var scope = $rootScope.$new();
                $compile($("#mainNavigationView").html(html))(scope);
            });
            */
            $scope.refreshPreviousSession(previousSession.Tenant);
        } else {
            return window.open('/','_self');
        }
    });

    $scope.$on("LoggedIn", function() {
        startObservingUserActivtyThroughMouseAndKeyboard();
        startCheckingIfSessionIsInactive();
    });
    
    $scope.refreshPreviousSession = function (tenant) {
        //Refresh session and go somewhere
        console.log('refreshPreviousSession',tenant)
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
        ResourceStringsService.GetInternalResourceStringsForLocale(locale).then(function(result) {
            $scope.copyrightString = ResourceUtility.getString('FOOTER_COPYRIGHT', [(new Date()).getFullYear()]);
            $scope.privacyPolicyString = ResourceUtility.getString('HEADER_PRIVACY_POLICY');
        });
    };
    
    $scope.privacyPolicyClick = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        HelpService.OpenPrivacyPolicy();
    };

    function startObservingUserActivtyThroughMouseAndKeyboard() {
        console.log('startObservingUserActivtyThroughMouseAndKeyboard');
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
        console.log('startCheckingIfSessionIsInactive');
        refreshSessionLastActiveTimeStamp();
        inactivityCheckingId = setInterval(
            checkIfSessionIsInactiveEveryInterval, 
            TIME_INTERVAL_BETWEEN_INACTIVITY_CHECKS
        ); // 1 minute
    }

    function checkIfSessionIsInactiveEveryInterval() {
        //console.log('checkIfSessionIsInactiveEveryInterval');
        if (Date.now() - BrowserStorageUtility.getSessionLastActiveTimestamp() >= TIME_INTERVAL_INACTIVITY_BEFORE_WARNING) {
            if (!warningModalInstance) {
                cancelCheckingIfSessionIsInactiveAndSetIdToNull();
                openWarningModal();
            }
            $timeout(callWhenWarningModalExpires, TIME_INTERVAL_WARNING_BEFORE_LOGOUT);
        }
    }
    
    function mustUserChangePassword(loginDocument) {
        var isTimedOut = loginDocument.MustChangePassword || TimestampIntervalUtility.isTimestampFartherThanNinetyDaysAgo(loginDocument.PasswordLastModified);

        console.log('mustUserChangePassword?',isTimedOut);
        return isTimedOut;
    }
    
    
    function hasSessionTimedOut() {
        var isTimedOut = Date.now() - BrowserStorageUtility.getSessionLastActiveTimestamp() >=
            TIME_INTERVAL_INACTIVITY_BEFORE_WARNING + TIME_INTERVAL_WARNING_BEFORE_LOGOUT;

        console.log('hasSessionTimedOut?',isTimedOut);
        return isTimedOut;
    }

    function refreshSessionLastActiveTimeStamp() {
        //console.log('refreshSessionLastActiveTimeStamp');
        BrowserStorageUtility.setSessionLastActiveTimestamp(Date.now());
    }

    function openWarningModal() {
        console.log('openWarningModal');
        warningModalInstance = $modal.open({
            animation: true,
            backdrop: false,
            scope: $scope,
            templateUrl: 'app/main/WarningModal.html'
        });

        $scope.refreshSession = function() {
            closeWarningModalAndSetInstanceToNull();
            startCheckingIfSessionIsInactive();
        };
    }
    
    function cancelCheckingIfSessionIsInactiveAndSetIdToNull() {
        console.log('cancelCheckingIfSessionIsInactiveAndSetIdToNull');
        clearInterval(inactivityCheckingId);
        inactivityCheckingId = null;
    }

    function stopObservingUserInteractionBasedOnMouseAndKeyboard() {
        console.log('stopObservingUserInteractionBasedOnMouseAndKeyboard');
        $(this).off("mousemove");
        $(this).off("keypress");
    }
    
    function callWhenWarningModalExpires() {
        console.log('callWhenWarningModalExpires');
        if (hasSessionTimedOut()) {
            $scope.sessionExpired = true;
            /** This line is actually necessary. Otherwise, user doesn't get properly logged out when tenant selection modal is up */
            //hideTenantSelectionModal();
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

    function closeWarningModalAndSetInstanceToNull() {
        console.log('closeWarningModalAndSetInstanceToNull');
        warningModalInstance.close();
        warningModalInstance = null;
    }
});

mainApp.factory('authInterceptor', function ($rootScope, $q, $window, BrowserStorageUtility) {
    return {
        request: function (config) {
            //console.log('authInterceptor req',config);
            config.headers = config.headers || {};
            if (BrowserStorageUtility.getTokenDocument()) {
                config.headers.Authorization = BrowserStorageUtility.getTokenDocument();
            }
            return config;
        },
        response: function (response) {
            //console.log('authInterceptor res',response);
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
