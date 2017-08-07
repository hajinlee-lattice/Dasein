angular.module('mainApp.appCommon.widgets.AnalyticAttributeTileWidget', [
    'mainApp.appCommon.utilities.ResourceUtility'
])

.controller('AnalyticAttributeTileWidgetController', function ($scope, ResourceUtility) {
    var widgetConfig = $scope.widgetConfig;
    var metadata = $scope.metadata;
    var data = $scope.data;
    
    $scope.ResourceUtility =  ResourceUtility;
    $scope.hasLift = data.Lift != null;
    $scope.showLift = data.ShowLift;
    $scope.aboveAverageLift = data.Lift != null && data.Lift > 1;
})

.directive('analyticAttributeTileWidget', function () {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/analyticAttributeTileWidget/AnalyticAttributeTileWidgetTemplate.html'
    };
  
    return directiveDefinitionObject;
});