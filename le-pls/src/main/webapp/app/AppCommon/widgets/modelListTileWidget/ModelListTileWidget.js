angular.module('mainApp.appCommon.widgets.ModelListTileWidget', [
    'mainApp.appCommon.utilities.EvergageUtility',
    'mainApp.appCommon.utilities.TrackingConstantsUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.DateTimeFormatUtility',
    'mainApp.core.utilities.GriotNavUtility',
    'mainApp.appCommon.services.WidgetFrameworkService'
])
.controller('ModelListTileWidgetController', function ($scope, $rootScope, $element, ResourceUtility, DateTimeFormatUtility, 
    EvergageUtility, TrackingConstantsUtility, GriotNavUtility, WidgetFrameworkService) {
    $scope.ResourceUtility = ResourceUtility;
    
    var widgetConfig = $scope.widgetConfig;
    var metadata = $scope.metadata;
    var data = $scope.data;
    
    if (widgetConfig == null || data == null) {
        return;
    }
    // adds a glowing border when hovered
    $(".panel-model-fancy").hover(function () {
        $(this).toggleClass("hover");
    });

    $(".panel .model-delete a").hover(function () {
        $(this).toggleClass("hover");
    });
    
    //TODO:pierce Field names subject to change
    $scope.displayName = data[widgetConfig.NameProperty];
    $scope.status = data[widgetConfig.StatusProperty];
    $scope.score = data[widgetConfig.ScoreProperty];
    if ($scope.score != null && $scope.score < 1) {
        $scope.score = Math.round($scope.score * 100);
    }
    $scope.externalAttributes = data[widgetConfig.ExternalAttributesProperty];
    $scope.internalAttributes = data[widgetConfig.InternalAttributesProperty];
    
    $scope.createdDate = null;
    var rawDate = data[widgetConfig.CreatedDateProperty];
    var convertedDate = DateTimeFormatUtility.ConvertCSharpDateTimeOffsetToJSDate(rawDate);
    if (convertedDate != null) {
        $scope.createdDate = convertedDate.toLocaleDateString();
    }
    
    $scope.tileClick = function () {
        $rootScope.$broadcast(GriotNavUtility.MODEL_DETAIL_NAV_EVENT, data);
    };
   
})
.directive('modelListTileWidget', function ($compile) {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/modelListTileWidget/ModelListTileWidgetTemplate.html'
    };
  
    return directiveDefinitionObject;
});