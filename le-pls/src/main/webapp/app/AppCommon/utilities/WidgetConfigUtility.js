angular.module('mainApp.appCommon.utilities.WidgetConfigUtility', [
])
.service('WidgetConfigUtility', function () {

    // Constants:

    // Widget types
    this.ANALYTIC_ATTRIBUTE_LIST_WIDGET = 'AnalyticAttributeListWidget';
    this.ANALYTIC_ATTRIBUTE_TILE_WIDGET = 'AnalyticAttributeTileWidget';
    this.ARC_CHART_WIDGET = 'ArcChartWidget';
    this.COLLAPSIBLE_PANEL_WIDGET = 'CollapsiblePanelWidget';
    this.GRID_WIDGET = 'GridWidget';
    this.LEAD_DETAILS_TILE_WIDGET = 'LeadDetailsTileWidget';
    this.MODEL_DETAILS_WIDGET = 'ModelDetailsWidget';
    this.MODEL_LIST_TILE_WIDGET = 'ModelListTileWidget';
    this.MODEL_LIST_CREATION_HISTORY = 'ModelListCreationHistoryWidget';
    this.REPEATER_WIDGET = 'RepeaterWidget';
    this.PLAY_DETAILS_TILE_WIDGET = 'PlayDetailsTileWidget';
    this.PLAY_LIST_TILE_WIDGET = 'PlayListTileWidget';
    this.RICH_TEXT_WIDGET = 'RichTextWidget';
    this.SCREEN_WIDGET = 'ScreenWidget';
    this.SCREEN_HEADER_WIDGET = 'ScreenHeaderWidget';
    this.SIMPLE_GRID_WIDGET = 'SimpleGridWidget';
    this.SIMPLE_TAB_WIDGET = 'SimpleTabWidget';
    this.TALKING_POINT_WIDGET = 'TalkingPointWidget';
    this.TEST_WIDGET = 'TestWidget';
    this.TAB_WIDGET = 'TabWidget';
    this.THRESHOLD_EXPLORER_WIDGET = 'ThresholdExplorerWidget';
    this.TOP_PREDICTOR_WIDGET = 'TopPredictorWidget';
    this.PERFORMANCE_TAB_WIDGET = 'PerformanceTabWidget';
    this.ADMIN_INFO_SUMMARY_WIDGET = 'AdminInfoSummaryWidget';
    this.ADMIN_INFO_ALERTS_WIDGET = 'AdminInfoAlertsWidget';
    this.LEADS_TAB_WIDGET = 'LeadsTabWidget';
    this.USER_MANAGEMENT_WIDGET = 'UserManagementWidget';

    // Values for "ActiveWidgets" WidgetConfig property
    this.ACTIVE_WIDGET_ALL = "All";
    this.ACTIVE_WIDGET_EVEN = "Even";
    this.ACTIVE_WIDGET_FIRST = "First";
    this.ACTIVE_WIDGET_LAST = "Last";
    this.ACTIVE_WIDGET_NONE = "None";
    this.ACTIVE_WIDGET_ODD = "Odd";

    this.GetWidgetConfig = function (applicationWidgetConfig, id) {
        if (applicationWidgetConfig == null) {
            return null;
        }

        if (id == null || id === "") {
            return null;
        }

        if (applicationWidgetConfig.ID === id) {
            return applicationWidgetConfig;
        }

        // this widget is not a match and has no children to check
        if (applicationWidgetConfig.Widgets == null) {
            return null;
        }

        // check if any children match
        for (var i = 0; i < applicationWidgetConfig.Widgets.length; i++) {
            var child = applicationWidgetConfig.Widgets[i];
            var matchedChild = this.GetWidgetConfig(child, id);
            if (matchedChild !== null) {
                return matchedChild;
            }
        }

        return null;
    };
});