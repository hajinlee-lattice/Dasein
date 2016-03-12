angular.module('mainApp.appCommon.widgets.ScreenWidget', [
    'mainApp.appCommon.services.WidgetFrameworkService'
])

.controller('ScreenWidgetController', function ($scope, $element, WidgetFrameworkService) {
    
    var widgetConfig = $scope.widgetConfig;
    var metadata = $scope.metadata;
    var data = $scope.data;
    var parentData = $scope.parentData;
    
    var container = $('<div></div>');
    $($element).append(container);

    var options = {
        element: container,
        widgetConfig: widgetConfig,
        metadata: metadata,
        data: data,
        parentData: parentData
    };
    WidgetFrameworkService.CreateChildWidgets(options, $scope.data);
})

.directive('screenWidget', function () {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/screenWidget/ScreenWidgetTemplate.html'
    };
  
    return directiveDefinitionObject;
});