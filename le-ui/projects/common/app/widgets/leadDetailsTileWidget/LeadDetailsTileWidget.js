angular.module('mainApp.appCommon.widgets.LeadDetailsTileWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.services.WidgetFrameworkService'
])

.controller('LeadDetailsTileWidgetController', function ($scope, ResourceUtility, WidgetFrameworkService) {
    $scope.header = ResourceUtility.getString('DANTE_LEAD_TILE_HEADER');
    
    var widgetConfig = $scope.widgetConfig;
    var metadata = $scope.metadata;
    var data = $scope.data;
    
    var leadTileScoreContainer = $('.js-dante-lead-tile-score', $scope.element);
    var options = {
        element: leadTileScoreContainer,
        widgetConfig: widgetConfig,
        metadata: metadata,
        data: data
    };
    WidgetFrameworkService.CreateChildWidgets(options, $scope.data);
})
.directive('leadDetailsTileWidget', function ($compile) {
    var directiveDefinitionObject = {
        templateUrl: '/app/widgets/leadDetailsTileWidget/LeadDetailsTileWidgetTemplate.html'
    };
  
    return directiveDefinitionObject;
});