//Initial load of the application
angular.module('mainApp', [
    'ngRoute',
    'ngSanitize',
    'ngAnimate',
    'ngDropdowns',
    'templates-main',
    'common.utilities.browserstorage',
    'mainApp.appCommon.utilities.MetadataUtility',
    'mainApp.core.utilities.ServiceErrorUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.core.services.ResourceStringsService',
    'mainApp.core.services.SessionService',
    'mainApp.core.services.ConfigurationService',
    'mainApp.core.services.DanteWidgetService',
    'mainApp.core.services.NotionService',
    'mainApp.core.services.LpiPreviewService',
    'mainApp.core.controllers.MainViewController',
    'mainApp.core.controllers.ServiceErrorController',
    'mainApp.core.controllers.NoNotionController',
    'mainApp.core.controllers.NoAssociationController'
])
.controller('MainController', function (
    $scope, $rootScope, $compile, $http, URLUtility, BrowserStorageUtility, ResourceUtility,
    ServiceErrorUtility, MetadataUtility, StringUtility, ResourceStringsService, SessionService, 
    ConfigurationService, DanteWidgetService, NotionService, LpiPreviewStore, AuthenticationUtility
) {

    // IE doesn't create the console object by default, only if the dev tools are open.
    if (typeof window.console == "undefined") {
        window.console = {
            "log":function(){}
        };
    }

    if (URLUtility.LpiPreview() == null) {
        // Adjust parent IFrame height to fit MainViewDiv height (not the best place for this)
        (function(self) {
            var inIframe = function() {
                try {
                    return window.self !== window.top;
                } catch (e) {
                    return true;
                }
            };

            if (!inIframe())
                return console.log('-!- No IFrame Detected.');

            var IFrameHeightCheck = function() {
                var MainViewDiv = document.getElementById('mainView');

                if (MainViewDiv) {
                    var height = MainViewDiv.offsetHeight;

                    if (typeof self.MVDHeight == 'undefined' || self.MVDHeight != height) {
                        if (height && height > 150) {
                            window.parent.postMessage("IFrameResizeEvent="+height, "*");
                        } else {
                            window.parent.postMessage("IFrameResizeEvent=auto", "*");
                        }

                        self.MVDHeight = height;
                    }
                }
            };

            IFrameHeightCheck();
            self.resizeTimer = setInterval(IFrameHeightCheck, 10); // FIXME - Interval covers all cases but is more resource intensive
        })(this);

        // Listen for when our Salesforce app dispatches Account or Lead selection events
        window.addEventListener('message', function (evt) {
            if (!evt || typeof evt.data !== 'string')
                return; // console.log('-!- Dante recieved a message - empty event payload', evt);

            var parms = evt.data.split('='),
                property = parms[0],
                value = parms[1],
                map = {
                    'CrmAccountSelectedEvent': 'account',
                    'CrmLeadSelectedEvent': 'lead',
                    'CrmRecommendationSelectedEvent': 'recommendation',
                    'CrmContactSelectedEvent': 'contact',
                    'CrmOpportunitySelectedEvent': 'opportunity',
                    'CrmTabSelectedEvent': 'tab'
                },
                type = map[property];

            if (type) {
                switch (type) {
                    case "tab":
                        $scope.handleCrmTabSelectedEvent(evt, value);
                        break;
                    case "account":
                        $scope.handleCrmAccountSelectedEvent(evt, type);
                        break;
                    default:
                        $scope.handleCrmObjectSelectedEvent(evt, type);
                        break;
                }
            }
        }, false);
    }

    $scope.handleCrmTabSelectedEvent = function (evt, newTitle) {
        var appElement = document.querySelector('div[data-ng-controller="TabWidgetController"]');

        if (!appElement || !newTitle)
            return false;

        var appScope = angular.element(appElement).scope();

        if (appScope) {
            appScope.$apply(function() {
                var tabs = appScope.tabs;

                if (!tabs || !tabs.length || tabs.length < 2)
                    return;

                var selected = 0;

                for (var i=0; i<tabs.length; i++) {
                    var tab = tabs[i];
                    var title = tab.Title.replace(' ','');

                    if (title.toLowerCase() === newTitle.toLowerCase()) {
                        selected = i;
                    }
                }

                appScope.tabClicked(null, tabs[selected]);
            });

            return false;
        }
    };

    $scope.handleCrmAccountSelectedEvent = function (evt, associationType) {
        var separatorIndex = evt.data.indexOf("=");
        var accountId = evt.data.substring(separatorIndex + 1, evt.data.length);
        $scope.getRootNotion(accountId);
    };

    $scope.handleCrmObjectSelectedEvent = function (evt, associationType) {
        var self = this;
        var separatorIndex = evt.data.indexOf("=");
        var questionMarkIndex = evt.data.indexOf("?");
        var objectId = null;
        var accountId = null;
        if (questionMarkIndex != -1) {
            objectId = evt.data.substring(separatorIndex + 1, questionMarkIndex);

            var otherString = evt.data.substring(questionMarkIndex + 1, evt.data.length);
            var otherStrings = otherString.split("?");
            // ]Get the AccountID property
            for (i = 0; i < otherStrings.length; i++) {
                var tokens = otherStrings[i].split("=");
                if (tokens.length < 2) {
                    continue;
                }
                if (tokens[0] == "AccountID") {
                    accountId = tokens[1];
                }
            }
        } else {
            objectId = evt.data.substring(separatorIndex + 1, evt.data.length);
        }

        $scope.getRootNotion(accountId, objectId, associationType);
    };

    $scope.validateSession = function () {
        AuthenticationUtility.AppendHttpHeaders($http);
        $scope.getResourceStrings();
        // SessionService.ValidateSession().then(function (result) {
        //     if (result != null && result.success === true) {
        //     } else {
        //         ServiceErrorUtility.ShowErrorView(result);
        //     }
        // });
    };

    $scope.getResourceStrings = function () {
        ResourceStringsService.GetResourceStrings(ResourceUtility.DefaultLocale).then(function (result) {
            $scope.getConfiguration();
        });
    };

    function escapeJsonString(val) {
        return val.replace(/\n/g , "\\n").replace(/\r/g, "\\r");
    }

    $scope.getConfiguration = function () {
        // Get the CRM custom settings string from the URL and store it for later use
        var customSettingsQueryString = URLUtility.CustomSettings();
        if (!StringUtility.IsEmptyString(customSettingsQueryString)) {

            // Need to escape the query string first because new line characters are not properly escaped (ENG-7563)
            var customSettingsObj = JSON.parse(escapeJsonString(customSettingsQueryString));
            BrowserStorageUtility.setCrmCustomSettings(customSettingsObj);

            if (customSettingsObj != null && customSettingsObj.hideNavigation === true) {
                $('body').addClass('dante-hide-navigation');
            }
        }

        ConfigurationService.GetConfiguration().then(function (result) {
            var associationType = null;
            if (result != null && result.success === true) {
                var objectId = null;
                if (URLUtility.LpiPreview() !== null) {
                    associationType = "recommendation";
                } else if (URLUtility.CrmPurchaseHistoryAccount() !== null && URLUtility.CrmRecommendation() === null){
                    associationType = "purchaseHistory";
                } else if (URLUtility.CrmLead() != null) {
                    objectId = URLUtility.CrmLead();
                    associationType = "lead";
                } else if (URLUtility.CrmRecommendation() != null) {
                    objectId = URLUtility.CrmRecommendation();
                    associationType = "recommendation";
                } else if (URLUtility.CrmContact() != null) {
                    objectId = URLUtility.CrmContact();
                    associationType = "contact";
                } else if (URLUtility.CrmOpportunity() != null) {
                    objectId = URLUtility.CrmOpportunity();
                    associationType = "opportunity";
                } else {
                    associationType = "account";
                }
                $scope.getRootNotion(URLUtility.CrmAccount(), objectId, associationType);
            } else {
                ServiceErrorUtility.ShowErrorView(result);
            }
        });
    };

    // Need to determine what context we are working with (Account vs. Lead vs. Contact)
    $scope.setApplicationContext = function (accountId, objectId, associationType) {
        var idToReturn = null;
        // Determine root application context
        var leadQueryString = URLUtility.CrmLead();
        var recommendationQueryString = URLUtility.CrmRecommendation();
        var contactQueryString = URLUtility.CrmContact();
        var opportunityQueryString = URLUtility.CrmOpportunity();
        var purchaseHistoryAccountString = URLUtility.CrmPurchaseHistoryAccount();
        var lpiPreviewString = URLUtility.LpiPreview();
        if (lpiPreviewString === 'true') {
            BrowserStorageUtility.setRootApplication(MetadataUtility.ApplicationContext.Account);
        } else if (purchaseHistoryAccountString && !recommendationQueryString) {
            idToReturn = purchaseHistoryAccountString;
            objectId = purchaseHistoryAccountString;
            BrowserStorageUtility.setRootApplication(MetadataUtility.ApplicationContext.PurchaseHistory);
        } else if (accountId == null && !recommendationQueryString) {
            idToReturn = objectId;

            if (leadQueryString != null || contactQueryString != null || opportunityQueryString != null) {
                BrowserStorageUtility.setRootApplication(MetadataUtility.ApplicationContext.Lead);
            }
        } else {
            idToReturn = accountId || objectId;
            BrowserStorageUtility.setRootApplication(MetadataUtility.ApplicationContext.Account);
        }

        // Determine root application context
        var notionName = null;

        if (leadQueryString != null || contactQueryString != null || opportunityQueryString != null) {
            notionName = MetadataUtility.NotionNames.Lead;
        } else {
            notionName = MetadataUtility.NotionNames.Account;
        }

        // getSelectedCrmObject() is only used when clicking [<< Selling Ideas]
        // borrowing this for BIS Account page purchase history
        // Set the CrmObject to be used in the PlayDetailsControl
        var crmObject = {
            ID: objectId,
            Notion: notionName,
            associationType: associationType
        };

        BrowserStorageUtility.setSelectedCrmObject(crmObject);

        return idToReturn;
    };

    $scope.getRootNotion = function (accountId, objectId, associationType) {
        var idToFetch = $scope.setApplicationContext(accountId, objectId, associationType);
        // The main application notion can be changed so it should be fetched from the widget configuration
        var applicationWidgetConfig = DanteWidgetService.GetApplicationWidgetConfig();
        if (applicationWidgetConfig == null) {
            return null;
        }

        var allowPurchaseHistoryCallback = function (result) {
            if (result != null) {
                if (result.resultObj !== null) {
                    NotionService.setRootNotion(result.resultObj, applicationWidgetConfig);
                    BrowserStorageUtility.setRootApplication(MetadataUtility.ApplicationContext.PurchaseHistory);
                    $scope.showMainView();
                } else {
                    $scope.showNoAssociationView();
                }
            } else {
                ServiceErrorUtility.ShowErrorView(result);
            }
        };

        var callback = function (result) {
            if (result != null) {// && result.success === true) {
                if (result.resultObj != null) {
                    NotionService.setRootNotion(result.resultObj, applicationWidgetConfig);
                    $scope.showMainView();
                } else {
                    var purchaseHistoryAccountString = URLUtility.CrmPurchaseHistoryAccount();
                    var customSettings = BrowserStorageUtility.getCrmCustomSettings();

                    // if purchase history enabled, allow to load ui for purchase history
                    if (purchaseHistoryAccountString !== null && customSettings.ShowPurchaseHistory === true) {
                        NotionService.findOne('DanteAccount', purchaseHistoryAccountString).then(allowPurchaseHistoryCallback);
                    } else {
                        $scope.showNoAssociationView();
                    }
                }
            } else {
                ServiceErrorUtility.ShowErrorView(result);
            }
        };

        function handleLpiPreview(evt) {
            var data = angular.isObject(evt.data) ? evt.data : {};
            if (data.context !== 'lpipreview') {
                return;
            }

            if (data.notion && data.notion === 'lead') {
                LpiPreviewStore.setLead(data.notionObject);
                callback({
                    resultObj: data.notionObject
                });
            }
        }

        if (associationType === 'recommendation' && URLUtility.LpiPreview() !== null) {
            window.addEventListener('message', handleLpiPreview, false);
            if (window.self !== window.parent) {
                window.parent.postMessage('initLpiPreview', '*');
            }
            $scope.$on('$destroy', function() {
                window.removeEventListener('message', handleLpiPreview);
            });
        } else if (associationType == 'recommendation') {
            NotionService.findOneByKey(MetadataUtility.NotionNames.Lead, idToFetch, "RecommendationID", 1).then(callback);
        } else {
            NotionService.findOne(applicationWidgetConfig.Notion, idToFetch).then(callback);
        }
    };

    $scope.showMainView = function () {
        $http.get('app/core/views/MainView.html').success(function (html) {
            var scope = $rootScope.$new();
            $compile($("#mainView").html(html))(scope);
        });
    };

    $scope.validateSession();

    // Show a message when we get an unhandled service exception
    $scope.$on("ShowServiceErrorView", function (event, data) {
        $http.get('app/core/views/ServiceErrorView.html').success(function (html) {
            var scope = $rootScope.$new();
            $compile($("#mainView").html(html))(scope);
        });
    });

    $scope.showNoAssociationView = function () {
        $http.get('app/core/views/NoAssociationView.html').success(function (html) {
            var scope = $rootScope.$new();
            $compile($("#mainView").html(html))(scope);
        });
    };
})
.factory('CacheTemplate', function ($templateCache) {
    var cacher = {
        request: function (config) {
            if (!config.cache && /(\.html$)/.test(config.url)) {
                config.cache = $templateCache;
            }
            return config;
        }
    };

    return cacher;
})
.config(function ($httpProvider) {
    $httpProvider.interceptors.push('CacheTemplate');
});
