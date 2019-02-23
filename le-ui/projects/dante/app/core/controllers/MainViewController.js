angular.module('mainApp.core.controllers.MainViewController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.MetadataUtility',
    'mainApp.appCommon.utilities.URLUtility',
    'common.utilities.browserstorage',
    'mainApp.core.utilities.ServiceErrorUtility',
    'mainApp.core.controllers.MainHeaderController',
    'mainApp.core.services.DanteWidgetService',
    'mainApp.core.services.NotionService',
    'mainApp.plays.controllers.PlayListController',
    'mainApp.plays.controllers.PlayDetailsController',
    'mainApp.leads.controllers.LeadDetailsController',
    'mainApp.purchaseHistory.controllers.PurchaseHistoryController',
    'mainApp.appCommon.widgets.AnalyticAttributeTileWidget',
    'mainApp.appCommon.widgets.AnalyticAttributeListWidget',
    'mainApp.appCommon.widgets.PlayListTileWidget',
    'mainApp.appCommon.widgets.PlayDetailsTileWidget',
    'mainApp.appCommon.widgets.LeadDetailsTileWidget',
    'mainApp.appCommon.widgets.TalkingPointWidget',
    'mainApp.appCommon.widgets.PurchaseHistoryWidget'
])

.controller('MainViewController', function ($scope, $rootScope, $compile, $http, ResourceUtility,
    BrowserStorageUtility, DanteWidgetService, MetadataUtility, NotionService, URLUtility, ServiceErrorUtility) {

    // Hide header if it is set in the custom settings
    var customSettings = BrowserStorageUtility.getCrmCustomSettings();
    $scope.showMainHeader = (customSettings && customSettings.HideHeader) ? false : true;

    var selectedCrmObject = BrowserStorageUtility.getSelectedCrmObject();
    if (BrowserStorageUtility.getRootApplication() === MetadataUtility.ApplicationContext.PurchaseHistory) {
        showPurchaseHistory();
    } else if (selectedCrmObject.associationType == 'recommendation') {
        var RootObject = BrowserStorageUtility.getCurrentRootObject();
        var SalesforceAccountID = RootObject.SalesforceAccountID;

        if (SalesforceAccountID) {
            var applicationWidgetConfig = DanteWidgetService.GetApplicationWidgetConfig();

            NotionService.findOne('DanteAccount', SalesforceAccountID).then(function(result) {
                if (result != null && result.success === true) {
                    if (result.resultObj != null) {
                        NotionService.setRootNotion(result.resultObj, applicationWidgetConfig);
                        showPlayDetails(RootObject.PlayID, true);
                    }
                } else {
                    ServiceErrorUtility.ShowErrorView(result);
                }
            });
        } else {
            showPlayDetails(RootObject.PlayID, true);
        }
    } else if (BrowserStorageUtility.getRootApplication() == MetadataUtility.ApplicationContext.Lead) {
        showLeadDetails();
    } else if (BrowserStorageUtility.getRootApplication() == MetadataUtility.ApplicationContext.Account && selectedCrmObject && selectedCrmObject.ID != null) {
        //TODO:pierce There is logic in PlayControl that will need to be moved into PlayDetailsControler
        showPlayDetails(selectedCrmObject.ID, true);
    } else {
        showPlayList();
    }

    function setPageClassName (page) {
        setTimeout(function() {
            $('body')
                .removeClass('page-lead-details')
                .removeClass('page-play-details')
                .removeClass('page-play-list')
                .removeClass('page-purchase-history')
                .addClass('page-' + page);
        }, 0);
    }

    function showLeadDetails () {
        $http.get('app/leads/views/LeadDetailsView.html').success(function (html) {
            var scope = $rootScope.$new();
            $compile($("#mainContentView").html(html))(scope);
            setPageClassName('lead-details');
        });
    }

    // Handle when the Lead Details should be shown
    $scope.$on("ShowLeadDetails", function (event, data) {
        showLeadDetails();
    });

    function showPlayList () {
        $http.get('app/plays/views/PlayListView.html').success(function (html) {
            var scope = $rootScope.$new();
            $compile($("#mainContentView").html(html))(scope);
            setPageClassName('play-list');
        });
    }

    // Handle when the Play List should be shown
    $scope.$on("ShowPlayList", function (event, data) {
        showPlayList();
    });

    function showPlayDetails (playId, isLatticeForLeadsContext) {
        isLatticeForLeadsContext = isLatticeForLeadsContext != null && typeof isLatticeForLeadsContext === "boolean" ? isLatticeForLeadsContext : false;
        $http.get('app/plays/views/PlayDetailsView.html', { port: ':8443' }).success(function (html) {
            var scope = $rootScope.$new();
            scope.playId = playId;
            scope.isLatticeForLeadsContext = isLatticeForLeadsContext;
            $compile($("#mainContentView").html(html))(scope);
            setPageClassName('play-details');
        });
    }

    // Handle when the Play List should be shown
    $scope.$on("ShowPlayDetails", function (event, data) {
        showPlayDetails(data);
    });

    function showPurchaseHistory (data) {
        $http.get('app/purchaseHistory/views/PurchaseHistoryView.html').success(function (html) {
            var scope = $rootScope.$new();
            $compile($("#mainContentView").html(html))(scope);
            setPageClassName('purchase-history');
        });
    }

});