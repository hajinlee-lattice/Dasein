angular.module('mainApp.appCommon.widgets.PlayListTileWidget', [
    'mainApp.appCommon.utilities.EvergageUtility',
    'mainApp.appCommon.utilities.TrackingConstantsUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'mainApp.appCommon.services.PlayTileService'
])
.controller('PlayListTileWidgetController', function ($scope, $element, EvergageUtility, TrackingConstantsUtility, WidgetFrameworkService, PlayTileService) {
    
    var widgetConfig = $scope.widgetConfig;
    var metadata = $scope.metadata;
    var data = $scope.data;
    
    var successCallback = function (tileData) {
        $scope.tileData = tileData;
        var tileElement = $($element);
        var tileScoreContainer = $('.js-dante-play-tile-score', tileElement);
        var options = {
            element: tileScoreContainer,
            widgetConfig: widgetConfig,
            metadata: metadata,
            data: data
        };
        WidgetFrameworkService.CreateChildWidgets(options, $scope.data);
    };
    PlayTileService.GetTileData(data, widgetConfig, metadata, successCallback);
    
    $scope.playNameClicked = function () {
        EvergageUtility.TrackAction(TrackingConstantsUtility.PLAY_LIST_TILE_CLICKED);
        window.location.hash = '/Main/PlayDetails/' + $scope.tileData.ID;
    };
})
.directive('playListTileWidget', function ($compile) {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/playListTileWidget/PlayListTileWidgetTemplate.html'
    };
  
    return directiveDefinitionObject;
});