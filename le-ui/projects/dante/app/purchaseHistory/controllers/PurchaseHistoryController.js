angular.module('mainApp.purchaseHistory.controllers.PurchaseHistoryController', [
    'mainApp.appCommon.utilities.WidgetConfigUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'common.utilities.browserstorage',
    'mainApp.core.services.DanteWidgetService'
])
.controller('PurchaseHistoryController', function ($scope, $rootScope, WidgetConfigUtility, 
    BrowserStorageUtility, WidgetFrameworkService, DanteWidgetService) {

    var metadata = BrowserStorageUtility.getMetadata();
    var widgetConfig = DanteWidgetService.GetApplicationWidgetConfig();
    var data = BrowserStorageUtility.getCurrentRootObject();

    // using tab widget to mimic LatticeForAccounts application size
    var purchaseHistoryTabConfig = WidgetConfigUtility.GetWidgetConfig(
        widgetConfig,
        "purchaseHistoryTabWidget"
    );

    // PurchaseHistoryWidgetController will take over
    var purchaseHistoryContainer = $('#purchaseHistoryScreen');
    WidgetFrameworkService.CreateWidget({
        element: purchaseHistoryContainer,
        widgetConfig: purchaseHistoryTabConfig,
        metadata: metadata,
        data: data
    });
});