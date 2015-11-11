angular.module('mainApp.appCommon.widgets.CollapsiblePanelWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.MetadataUtility',
    'mainApp.appCommon.services.WidgetFrameworkService'
])

.controller('CollapsiblePanelWidgetController', function ($scope, MetadataUtility, ResourceUtility, WidgetFrameworkService) {
    var widgetConfig = $scope.widgetConfig;
    var metadata = $scope.metadata;
    var data = $scope.data;
    var parentData = $scope.parentData;
    var isActive = $scope.isActive;
    isActive = (typeof isActive === 'undefined') ? true : isActive;

    if (data == null) {
        return;
    }

    var contentContainer = $('.js-collapsible-panel-content', $scope.element);

    $scope.panelOpen = isActive;

    // If panel starts closed, call slideToggle() to correctly set it's visibility
    // Don't do this if panel starts opened - it will undo it
    if (!$scope.panelOpen) {
        contentContainer.slideToggle();
    }

    // Find the appropriate notion metadata
    var notionMetadata = MetadataUtility.GetNotionMetadata(widgetConfig.Notion, metadata);

    // Getting Title of CollapsiblePanel
    var titleProperty = MetadataUtility.GetNotionProperty(widgetConfig.TitleProperty, notionMetadata);
    $scope.panelTitle = "";
    if (titleProperty != null && titleProperty.PropertyTypeString == MetadataUtility.PropertyType.STRING) {
        $scope.panelTitle = data[titleProperty.Name] || "";
    } else if (widgetConfig.TitleString != null) {
        $scope.panelTitle = ResourceUtility.getString(widgetConfig.TitleString);
    }

    $scope.panelClicked = function () {
        $scope.panelOpen = !$scope.panelOpen;
        contentContainer.slideToggle();
    };

    var options = {
        scope: $scope,
        element: contentContainer,
        widgetConfig: widgetConfig,
        metadata: metadata,
        data: data,
        parentData: parentData
    };
    WidgetFrameworkService.CreateChildWidgets(options, data);
})

.directive('collapsiblePanelWidget', function () {
    var directiveDefinitionObject = {
        templateUrl: '/app/widgets/collapsiblePanelWidget/CollapsiblePanelWidgetTemplate.html'
    };

    return directiveDefinitionObject;
});