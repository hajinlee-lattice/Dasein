angular.module('mainApp.appCommon.widgets.PlayDetailsTileWidget', [
    'mainApp.appCommon.utilities.WidgetEventConstantUtility',
    'common.utilities.browserstorage',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'mainApp.appCommon.services.PlayTileService'
])
.controller('PlayDetailsTileWidgetController', function ($scope, $rootScope, $element,
    WidgetEventConstantUtility, BrowserStorageUtility, WidgetFrameworkService, PlayTileService) {

    var widgetConfig = $scope.widgetConfig;
    var metadata = $scope.metadata;
    var data = $scope.data;

    // Check the CRM custom settings and if it is configured then they will take precedence
    var showScore = true;
    var customSettings = BrowserStorageUtility.getCrmCustomSettings();
    if (customSettings != null && customSettings.ShowScore != null) {
        showScore = customSettings.ShowScore;
    }
    $scope.showScore = showScore;

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