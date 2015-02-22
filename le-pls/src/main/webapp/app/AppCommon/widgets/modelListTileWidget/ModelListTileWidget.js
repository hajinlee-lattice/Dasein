angular.module('mainApp.appCommon.widgets.ModelListTileWidget', [
    'mainApp.appCommon.utilities.EvergageUtility',
    'mainApp.appCommon.utilities.TrackingConstantsUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.DateTimeFormatUtility',
    'mainApp.core.utilities.GriotNavUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'mainApp.models.services.ModelService'
])
.controller('ModelListTileWidgetController', function ($scope, $rootScope, $element, ResourceUtility, DateTimeFormatUtility, 
    EvergageUtility, TrackingConstantsUtility, GriotNavUtility, WidgetFrameworkService) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.nameStatus = {
        editing: false
    };
    $scope.mayEditModels = true;

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
        //Changing the name of the model
        $scope.nameStatus.editing = true;
    };

    $scope.tileClick = function ($event) {
        if ($event != null && !$scope.nameStatus.editing) {
            $event.preventDefault();
        }

        var targetElement = $($event.target);
        if (targetElement.hasClass("fa-trash-o")) {
            // deleting the model
        } else if (!$scope.nameStatus.editing) {
            $rootScope.$broadcast(GriotNavUtility.MODEL_DETAIL_NAV_EVENT, data);
        }
    };
   
})
.controller('ChangeModelNameController', function ($scope, $rootScope, GriotNavUtility, ModelService) {
    $scope.data = {name: $scope.$parent.displayName};
    $scope.submitting = false;
    $scope.showNameEditError = false;

    $scope.closeErrorClick = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        $scope.showPasswordError = false;
    };

    $scope.submit = function($event) {
        if ($event != null) $event.stopPropagation();

        $scope.showNameEditError = false;

        if ($scope.submitting) return;
        $scope.submitting = true;

        ModelService.ChangeModelName($scope.$parent.data.Id, $scope.data.name).then(function(result) {
            if (result.Success) {
                $rootScope.$broadcast(GriotNavUtility.MODEL_LIST_NAV_EVENT, {});
            } else {
                $scope.nameEditErrorMessage = result.ResultErrors;
                $scope.showNameEditError = true;
                $scope.submitting = false;
            }
        });
    };

    $scope.cancel = function($event) {
        if ($event != null) $event.stopPropagation();
        $scope.showNameEditError = false;
        $scope.data = {name: $scope.$parent.displayName};
        $scope.nameStatus.editing = false;
    };
})
.directive('modelListTileWidget', function ($compile) {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/modelListTileWidget/ModelListTileWidgetTemplate.html'
    };

    return directiveDefinitionObject;
});