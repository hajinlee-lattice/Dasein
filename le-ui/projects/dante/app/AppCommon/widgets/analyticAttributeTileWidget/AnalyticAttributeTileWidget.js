angular.module('mainApp.appCommon.widgets.AnalyticAttributeTileWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'common.utilities.browserstorage'
])

.controller('AnalyticAttributeTileWidgetController', function ($scope, ResourceUtility, BrowserStorageUtility) {
    var widgetConfig = $scope.widgetConfig;
    var metadata = $scope.metadata;
    var data = $scope.data;
    
    // Check the CRM custom settings and if it is configured then they will take precedence
    var customSettings = BrowserStorageUtility.getCrmCustomSettings();
    if (customSettings != null && customSettings.ShowLift != null) {
        $scope.showLift = customSettings.ShowLift;
    } else {
        $scope.showLift = data.ShowLift;
    }
    
    $scope.ResourceUtility =  ResourceUtility;
    $scope.hasLift = data.Lift != null;
    $scope.aboveAverageLift = data.Lift != null && data.Lift > 1;
    $scope.icon = $scope.aboveAverageLift ? 'up-arrow-dots' : 'down-arrow-dots';
})

.directive('analyticAttributeTileWidget', function () {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/analyticAttributeTileWidget/AnalyticAttributeTileWidgetTemplate.html'
    };
  
    return directiveDefinitionObject;
});