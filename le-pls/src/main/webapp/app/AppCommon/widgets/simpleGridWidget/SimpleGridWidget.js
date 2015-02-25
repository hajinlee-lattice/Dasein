angular.module('mainApp.appCommon.widgets.SimpleGridWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.services.WidgetFrameworkService'
])

.controller('SimpleGridWidgetController', function ($scope, $element, ResourceUtility, WidgetFrameworkService) {
    
    var widgetConfig = $scope.widgetConfig;
    var data = $scope.data;
    var parentData = $scope.parentData;
    
    if (widgetConfig == null || widgetConfig.length === 0) {
        return;
    }
    
    $scope.gridTitle = ResourceUtility.getString(widgetConfig.TitleString);
    
    if (widgetConfig.Columns == null || widgetConfig.Columns.length === 0) {
        return;
    }
    
    
    $scope.columnTitles = [];
    $scope.columns = [];
    for (var i = 0; i < widgetConfig.Columns.length; i++) {
        var column = widgetConfig.Columns[i];
        $scope.columnTitles.push(ResourceUtility.getString(column.TitleString));
        $scope.columns.push(column.PropertyName);
    }
    
    $scope.columnData = data[widgetConfig.DataProvider];
    
})

.directive('simpleGridWidget', function () {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/simpleGridWidget/SimpleGridWidgetTemplate.html'
    };
  
    return directiveDefinitionObject;
});