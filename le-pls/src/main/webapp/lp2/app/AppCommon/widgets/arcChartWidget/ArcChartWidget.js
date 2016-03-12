angular.module('mainApp.appCommon.widgets.ArcChartWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.MetadataUtility',
    'mainApp.appCommon.utilities.AnimationUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'mainApp.appCommon.directives.charts.ArcChartDirective'
])

.service('ArcChartService', function (WidgetConfigUtility, MetadataUtility, AnimationUtility) {
    
    this.CalculateArcColor = function (score) {
        if (score == null) {
            return null;
        }
        
        var highestRgb = {R:27, G:172, B:94};
        var midRangeRgb = {R:255, G:255, B:102};
        var lowestRgb = {R:178, G:0, B:0};
        
        var rgbColor = null;
        if (score > 50) {
            rgbColor = AnimationUtility.CalculateRgbBetweenValues(highestRgb, midRangeRgb, score);
            return AnimationUtility.ConvertRgbToHex(rgbColor.R, rgbColor.G, rgbColor.B);
        } else {
            // Need to double the score when it is below 50 
            // because we use a different color scale from 0-50
            score = score * 2;
            rgbColor = AnimationUtility.CalculateRgbBetweenValues(midRangeRgb, lowestRgb, score);
            return AnimationUtility.ConvertRgbToHex(rgbColor.R, rgbColor.G, rgbColor.B);
        }
    };
    
})

.controller('ArcChartWidgetController', function ($scope, ResourceUtility, MetadataUtility, WidgetFrameworkService, ArcChartService) {
    
    var widgetConfig = $scope.widgetConfig;
    var metadata = $scope.metadata;
    var data = $scope.data;
    
    $scope.ChartValue = 0;
    $scope.ChartTotal = 100;
    $scope.ChartLabel = null;
    $scope.ChartColor = "#FF0000";
    $scope.ChartSize = 100;
    
    // Find the appropriate notion metadata
    var notionMetadata = MetadataUtility.GetNotionMetadata(widgetConfig.Notion, metadata);
    
    // Find Value from the metadata
    var valueProperty = MetadataUtility.GetNotionProperty(widgetConfig.ValueProperty, notionMetadata);
    if (valueProperty != null) {
        $scope.ChartValue = data[valueProperty.Name] || null;
        if ($scope.ChartValue != null) {
            $scope.ChartValue = Math.round($scope.ChartValue);
        }
        $scope.ChartColor = ArcChartService.CalculateArcColor($scope.ChartValue);
    }
    
    // Find Label from the metadata if it has been defined
    var labelProperty = MetadataUtility.GetNotionProperty(widgetConfig.LabelProperty, notionMetadata);
    if (labelProperty != null) {
        $scope.ChartLabel = data[labelProperty.Name] || null;
    }
    
    if ($scope.ChartLabel == null && (widgetConfig.LabelResourceKey != null || widgetConfig.LabelResourceKey !== "")) {
        $scope.ChartLabel = ResourceUtility.getString(widgetConfig.LabelResourceKey);
    }
    
})

.directive('arcChartWidget', function ($compile) {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/arcChartWidget/ArcChartWidgetTemplate.html'
    };
  
    return directiveDefinitionObject;
});