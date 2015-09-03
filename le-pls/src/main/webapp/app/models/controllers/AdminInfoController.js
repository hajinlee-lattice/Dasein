angular.module('mainApp.models.controllers.AdminInfoController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.WidgetConfigUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'mainApp.appCommon.services.TopPredictorService',
    'mainApp.core.services.WidgetService',
    'mainApp.core.utilities.NavUtility',
    'mainApp.models.services.ModelService',
    'mainApp.core.services.FeatureFlagService'
])
.controller('AdminInfoController', function ($scope, $rootScope, $http, ResourceUtility, WidgetService,
    WidgetConfigUtility, WidgetFrameworkService, NavUtility, ModelService, FeatureFlagService) {
    $scope.ResourceUtility = ResourceUtility;

    var widgetConfig = WidgetService.GetApplicationWidgetConfig();
    if (widgetConfig == null) {
        return;
    }

    var screenWidgetConfig = WidgetConfigUtility.GetWidgetConfig(
        widgetConfig,
        "adminInfoScreenWidget"
    );

    if (screenWidgetConfig == null) {
        return;
    }

    var data = $scope.data;
    $scope.ModelId = data.ModelId;
    $scope.TenantId = data.TenantId;
    $scope.ModelHealthScore = data.ModelDetails.RocScore;
    $scope.TemplateVersion = data.ModelDetails.TemplateVersion;
    $scope.modelUploaded = data.ModelDetails.Uploaded;
    $scope.displayName = data.ModelDetails.DisplayName;

    $scope.onBackClicked = function() {
        var model = {
            Id: data.ModelId,
            DisplayName: data.ModelDetails.DisplayName,
            CreatedDate: data.ModelDetails.ConstructionTime,
            Status: data.ModelDetails.Status
        };
        $rootScope.$broadcast(NavUtility.MODEL_DETAIL_NAV_EVENT, model);
    };

    var flags = FeatureFlagService.Flags();
    var showAlertsTab = FeatureFlagService.FlagIsEnabled(flags.ADMIN_ALERTS_TAB);
    if (showAlertsTab) {
        $scope.loading= true;

        var suppressedCategories = data.SuppressedCategories;

        ModelService.GetModelAlertsByModelId(data.ModelId).then(function(result) {
            $scope.loading= false;
            if (result != null && result.success === true) {
                data.ModelAlerts = result.resultObj;
                data.SuppressedCategories = suppressedCategories;

                var contentContainer = $('#adminInfoContainer');
                WidgetFrameworkService.CreateWidget({
                    element: contentContainer,
                    widgetConfig: screenWidgetConfig,
                    metadata: null,
                    data: data,
                    parentData: null
                });
            }
        });
    } else {
        var screenWidgetConfigNoAlertsTab = angular.copy(screenWidgetConfig);
        for (var i = 0; i < screenWidgetConfigNoAlertsTab.Widgets.length; i++) {
            var widget = screenWidgetConfigNoAlertsTab.Widgets[i];
            if (widget.Tabs != null) {
                var tabs = [];
                for (var j = 0; j < widget.Tabs.length; j++) {
                    var tab = widget.Tabs[j];
                    if (tab.ID !== "adminInfoAlertsTab") {
                        var tabCopy = angular.copy(tab);
                        tabs.push(tabCopy);
                    }
                }
                widget.Tabs = tabs;
            }
        }

        var contentContainer = $('#adminInfoContainer');
        WidgetFrameworkService.CreateWidget({
            element: contentContainer,
            widgetConfig: screenWidgetConfigNoAlertsTab,
            metadata: null,
            data: data,
            parentData: null
        });
    }

});