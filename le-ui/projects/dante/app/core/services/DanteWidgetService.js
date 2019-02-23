angular.module('mainApp.core.services.DanteWidgetService', [
    'common.utilities.browserstorage',
    'mainApp.appCommon.utilities.WidgetConfigUtility',
    'mainApp.appCommon.utilities.MetadataUtility'
])
.service('DanteWidgetService', function ($compile, BrowserStorageUtility, WidgetConfigUtility, MetadataUtility, ServiceErrorUtility) {

    this.GetApplicationWidgetConfig = function () {
        var applicationId = BrowserStorageUtility.getRootApplication();
        if (applicationId == null) {
            return null;
        }

        var rootWidgetConfig = BrowserStorageUtility.getWidgetConfig();

        if (!rootWidgetConfig) {
            return ServiceErrorUtility.ShowErrorView({
                "resultErrors": "Error - Unable to get APPLICATION WIDGET CONFIGURATION, Application Context not set?"
            });
        }

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