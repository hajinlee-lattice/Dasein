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
    
    $scope.customGridClass = "";
    if (widgetConfig.CustomGridClass != null) {
        $scope.customGridClass = widgetConfig.CustomGridClass;
    }
    
    // Build up the list of column headings
    $scope.columnTitles = [];
    for (var i = 0; i < widgetConfig.Columns.length; i++) {
        var column = widgetConfig.Columns[i];
        var columnTitle = {
            title: ResourceUtility.getString(column.TitleString),
            subtitle: ResourceUtility.getString(column.SubtitleString)
        };
        $scope.columnTitles.push(columnTitle);
    }
    
    // Build up the list of rows and columns
    var gridBody = $('.js-simple-grid-rows', $element);
    var dataProvider = data[widgetConfig.DataProvider];
    for (var y = 0; y < dataProvider.length; y++) {
        var gridRow = $('<tr></tr>');
        gridBody.append(gridRow);
        var rowData = dataProvider[y];
        
        for (var x = 0; x < widgetConfig.Columns.length; x++) {
            var columnMetaData = widgetConfig.Columns[x];
            var gridColumn = $('<td></td>');
            var columnData = rowData[columnMetaData.PropertyName];
            
            switch (columnMetaData.DisplayType) {
                case "Boolean":
                    if (columnData === true) {
                        columnData = '<i class="fa fa-check"></i>';
                    } else {
                        columnData = '';
                    }
                    break;
            }
            gridColumn.html(columnData);
            gridRow.append(gridColumn);
        }
    }
    
    
})

.directive('simpleGridWidget', function () {
    var directiveDefinitionObject = {
        templateUrl: '/app/widgets/simpleGridWidget/SimpleGridWidgetTemplate.html'
    };
  
    return directiveDefinitionObject;
});