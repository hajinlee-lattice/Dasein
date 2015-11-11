angular.module('mainApp.appCommon.widgets.ScreenHeaderWidget', [
    'mainApp.appCommon.utilities.MetadataUtility',
    'mainApp.appCommon.services.WidgetFrameworkService'
])

.controller('ScreenHeaderWidgetController', function ($scope, MetadataUtility, WidgetFrameworkService) {
    
    var widgetConfig = $scope.widgetConfig;
    var metadata = $scope.metadata;
    var data = $scope.data;
    
    // Find the appropriate notion metadata
    var notionMetadata = MetadataUtility.GetNotionMetadata(widgetConfig.Notion, metadata);
    
    // Find Title from the metadata
    var titleProperty = MetadataUtility.GetNotionProperty(widgetConfig.TitleProperty, notionMetadata);
    if (titleProperty != null) {
        $scope.title = data[titleProperty.Name] || "";
    }
    
    // Find Subtitle from the metadata
    var subtitleProperty = MetadataUtility.GetNotionProperty(widgetConfig.SubtitleProperty, notionMetadata);
    if (subtitleProperty != null) {
        $scope.subtitle = data[subtitleProperty.Name] || "";
    }
})

.directive('screenHeaderWidget', function () {
    var directiveDefinitionObject = {
        templateUrl: '/app/widgets/screenHeaderWidget/ScreenHeaderWidgetTemplate.html'
    };
  
    return directiveDefinitionObject;
});