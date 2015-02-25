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
    
})

.directive('simpleGridWidget', function () {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/simpleGridWidget/SimpleGridWidgetTemplate.html'
    };
  
    return directiveDefinitionObject;
});