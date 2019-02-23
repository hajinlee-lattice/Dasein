angular.module('mainApp.appCommon.widgets.LeadDetailsTileWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.WidgetConfigUtility',
    'common.utilities.browserstorage',
    'mainApp.appCommon.services.WidgetFrameworkService'
])

.controller('LeadDetailsTileWidgetController', function ($scope, ResourceUtility, WidgetConfigUtility, BrowserStorageUtility, WidgetFrameworkService) {
    var widgetConfig = $scope.widgetConfig;
    var activeWidgets = widgetConfig.ActiveWidgets;
    $scope.header = ResourceUtility.getString('DANTE_LEAD_TILE_HEADER');
    
    // Check the CRM custom settings and if it is configured then they will take precedence
    var customSettings = BrowserStorageUtility.getCrmCustomSettings();
    if (customSettings != null && customSettings.ShowScore != null) {
        if (customSettings.ShowScore === true) {
            activeWidgets = [WidgetConfigUtility.ACTIVE_WIDGET_ALL];
        } else {
            activeWidgets = [WidgetConfigUtility.ACTIVE_WIDGET_NONE];
        }
    }
    
    if (activeWidgets !== undefined && activeWidgets[0].indexOf(WidgetConfigUtility.ACTIVE_WIDGET_NONE) === -1) {
        var metadata = $scope.metadata;
        var data = $scope.data;
        var leadTileScoreContainer = $('.js-dante-lead-tile-score', $scope.element);
        var options = {
            element: leadTileScoreContainer,
            widgetConfig: widgetConfig,
            metadata: metadata,
            data: data
        };
        WidgetFrameworkService.CreateChildWidgets(options, $scope.data);
    } else {
        $scope.style = function () {
            return {'line-height': '160px' };
        };

        $scope.style2 = function () {
            return { 'margin-top': '-120px' };
        };
    }
})
.directive('leadDetailsTileWidget', function ($compile) {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/leadDetailsTileWidget/LeadDetailsTileWidgetTemplate.html'
    };

    return directiveDefinitionObject;
});