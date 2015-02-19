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
    $scope.isActive = data[widgetConfig.StatusProperty] === "Active" ? true : false;
    $scope.createdDate = data[widgetConfig.CreatedDateProperty];
    
    $scope.modelNameEditClick = function ($event) {
        if ($event != null) {
            $event.stopPropagation();
        }
    };

    $scope.tileClick = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        
        var targetElement = $($event.target);
        if (targetElement.hasClass("fa-trash-o")) {
            // deleting the model
            
        } else {
            $rootScope.$broadcast(GriotNavUtility.MODEL_DETAIL_NAV_EVENT, data);
        }
        
        
    };
   
})
.directive('modelListTileWidget', function ($compile) {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/modelListTileWidget/ModelListTileWidgetTemplate.html'
    };
  
    return directiveDefinitionObject;
});