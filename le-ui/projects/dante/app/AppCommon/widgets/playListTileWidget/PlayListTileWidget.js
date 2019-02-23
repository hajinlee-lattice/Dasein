angular.module('mainApp.appCommon.widgets.PlayListTileWidget', [
    'common.utilities.browserstorage',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'mainApp.appCommon.services.PlayTileService'
])
.controller('PlayListTileWidgetController', function ($scope, $rootScope, $element, 
    BrowserStorageUtility, WidgetFrameworkService, PlayTileService) {

    var widgetConfig = $scope.widgetConfig;
    var metadata = $scope.metadata;
    var data = $scope.data;

    // Check the CRM custom settings and if it is configured then they will take precedence
    $scope.noScoreClass = null;
    var showScore = true;
    var customSettings = BrowserStorageUtility.getCrmCustomSettings();
    if (customSettings != null && customSettings.ShowScore != null) {
        showScore = customSettings.ShowScore;
    }

    if (customSettings != null && customSettings.ShowLift != null) {
        $scope.showLift = customSettings.ShowLift;
    } else {
        $scope.showLift = true;
    }

    $scope.showScore = showScore;

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

        if (showScore) {
            WidgetFrameworkService.CreateChildWidgets(options, $scope.data);
        } else {
            $scope.noScoreClass = "dante-play-tile-no-score";
        }
    };
    PlayTileService.GetTileData(data, widgetConfig, metadata, successCallback);

    $scope.playNameClicked = function () {
        $rootScope.$broadcast("ShowPlayDetails", $scope.tileData.ID);
    };
})
.directive('playListTileWidget', function ($compile) {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/playListTileWidget/PlayListTileWidgetTemplate.html'
    };

    return directiveDefinitionObject;
});