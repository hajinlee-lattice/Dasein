angular.module('mainApp.core.services.WidgetService', [
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.appCommon.utilities.WidgetConfigUtility',
    'mainApp.appCommon.utilities.MetadataUtility'
])
.service('WidgetService', function ($compile, BrowserStorageUtility, WidgetConfigUtility, MetadataUtility) {
    
    this.GetApplicationWidgetConfig = function () {
        var applicationId = "PredictiveLeadScoring";
        var rootWidgetConfig = BrowserStorageUtility.getWidgetConfigDocument();
        if (rootWidgetConfig.Applications != null) {
            for (var x = 0; x < rootWidgetConfig.Applications.length; x++) {
                var matchedWidget = WidgetConfigUtility.GetWidgetConfig(rootWidgetConfig.Applications[x], applicationId);
                if (matchedWidget != null) {
                    return matchedWidget;
                }
            }
        }
        return null;
    };
});