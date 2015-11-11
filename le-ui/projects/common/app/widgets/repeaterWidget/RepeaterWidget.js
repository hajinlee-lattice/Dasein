angular.module('mainApp.appCommon.widgets.RepeaterWidget', [
    'mainApp.appCommon.utilities.MetadataUtility',
    'mainApp.appCommon.utilities.SortUtility',
    'mainApp.appCommon.services.WidgetFrameworkService'
])

.controller('RepeaterWidgetController', function ($scope, $element, $q, MetadataUtility, SortUtility, WidgetFrameworkService) {
    var widgetConfig = $scope.widgetConfig;
    var metadata = $scope.metadata;
    var data = $scope.data;
    var parentData = $scope.parentData;

    var repeaterData = null;

    var targetNotionProperty = MetadataUtility.GetNotionAssociationMetadata(
                        widgetConfig.Notion, widgetConfig.TargetNotion, metadata);
    if (metadata != null && (targetNotionProperty == null || data[targetNotionProperty.Name] == null)) {
        return;
    }
    
    if (metadata == null) {
        repeaterData = parentData;
    } else {
        repeaterData = data[targetNotionProperty.Name];
    }
    

    // Order the data based on the Sorts specified in WidgetConfig
    var propsToSortOn = WidgetFrameworkService.GetSortProperties(widgetConfig, metadata);
    SortUtility.MultiSort(repeaterData, propsToSortOn);

    var numItems = repeaterData.length;
    var numCols = widgetConfig.Columns || 1;

    WidgetFrameworkService.CreateWidgetsInColumnLayout(
        $element,
        numItems,
        numCols,
        function (childElement, numItems, childIndex) {
            var activeWidgets = widgetConfig.ActiveWidgets;
            var isCurrentItemActive = WidgetFrameworkService.IsChildActive(
            activeWidgets, childIndex, repeaterData.length);

            var options = {
                element: childElement,
                widgetConfig: widgetConfig,
                metadata: metadata,
                data: data,
                parentData: parentData,
                isActive: isCurrentItemActive
            };
            var currentItemData = repeaterData[childIndex];
            // use 1 because columns are already accounted when repeating items
            var numberOfColumns = 1;
            WidgetFrameworkService.CreateChildWidgetsInColumnLayout(
                options, currentItemData, numberOfColumns); 
        }
    );
})

.directive('repeaterWidget', function () {
    var directiveDefinitionObject = {
        templateUrl: '/app/widgets/repeaterWidget/RepeaterWidgetTemplate.html'
    };

    return directiveDefinitionObject;
});