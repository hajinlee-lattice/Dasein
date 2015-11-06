angular.module('mainApp.appCommon.widgets.PlayDetailsTileWidget', [
    'mainApp.appCommon.utilities.EvergageUtility',
    'mainApp.appCommon.utilities.WidgetEventConstantUtility',
    'mainApp.appCommon.utilities.TrackingConstantsUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'mainApp.appCommon.services.PlayTileService'
])
.controller('PlayDetailsTileWidgetController', function ($scope, $rootScope, $element, EvergageUtility, TrackingConstantsUtility, WidgetEventConstantUtility, WidgetFrameworkService, PlayTileService) {
    
    var widgetConfig = $scope.widgetConfig;
    var metadata = $scope.metadata;
    var data = $scope.data;
    
    var successCallback = function (tileData) {
        $scope.tileData = tileData;
        var tileElement = $($element);
        var tileScoreContainer = $('.js-dante-play-details-nav-tile-score', tileElement);
        var options = {
            element: tileScoreContainer,
            widgetConfig: widgetConfig,
            metadata: metadata,
            data: data
        };
        WidgetFrameworkService.CreateChildWidgets(options, $scope.data);
    };
    PlayTileService.GetTileData(data, widgetConfig, metadata, successCallback);
    
    $scope.isSelected = data.isSelected;
    $scope.playNameClicked = function () {
        EvergageUtility.TrackAction(TrackingConstantsUtility.PLAY_DETAIL_TILE_CLICKED);
        $rootScope.$broadcast(WidgetEventConstantUtility.PLAY_DETAILS_TAB_CLICKED, $scope.tileData.ID);
    };
    
    // Handle selection event
    $scope.$on('PlayDetailsTabClickedEvent', function (event, data) { 
        $scope.isSelected = $scope.tileData.ID == data ? true : false;
    });
})
.directive('playDetailsTileWidget', function ($compile) {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/playDetailsTileWidget/PlayDetailsTileWidgetTemplate.html'
    };
  
    return directiveDefinitionObject;
});