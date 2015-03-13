angular.module('mainApp.appCommon.widgets.ModelListTileWidget', [
    'mainApp.appCommon.utilities.EvergageUtility',
    'mainApp.appCommon.utilities.TrackingConstantsUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.DateTimeFormatUtility',
    'mainApp.core.utilities.GriotNavUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.utilities.RightsUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'mainApp.models.services.ModelService',
    'mainApp.models.modals.DeleteModelModal'
])
.controller('ModelListTileWidgetController', function ($scope, $rootScope, $element, ResourceUtility, BrowserStorageUtility, RightsUtility, DateTimeFormatUtility,
    EvergageUtility, TrackingConstantsUtility, GriotNavUtility, WidgetFrameworkService, DeleteModelModal) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.nameStatus = {
        editing: false
    };
    var clientSession = BrowserStorageUtility.getClientSession();
    var widgetConfig = $scope.widgetConfig;
    var metadata = $scope.metadata;
    var data = $scope.data;
    $scope.mayEditModels = RightsUtility.mayEditModels(clientSession.availableRights);
    $scope.mayEditModelsClass = $scope.mayEditModels ? "model-name-editable" : "";
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
    $scope.isActive = data[widgetConfig.StatusProperty] === "Active";
    $scope.createdDate = data[widgetConfig.CreatedDateProperty];
    
    $scope.modelNameEditClick = function ($event) {
        if ($event != null) {
            $event.stopPropagation();
        }
        
        if ($scope.mayEditModels) {
            //Changing the name of the model
            $scope.nameStatus.editing = true;
        }
    };

    $scope.tileClick = function ($event) {
        if ($event != null && !$scope.nameStatus.editing) {
            $event.preventDefault();
        }

        var targetElement = $($event.target);
        if (targetElement.hasClass("fa-trash-o")) {
            DeleteModelModal.show($scope.data.Id);
        } else if (!$scope.nameStatus.editing) {
            $rootScope.$broadcast(GriotNavUtility.MODEL_DETAIL_NAV_EVENT, data);
        }
    };
    
})
.controller('ChangeModelNameController', function ($scope, $rootScope, GriotNavUtility, ResourceUtility, ModelService) {
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

        if ($scope.data.name.replace(/ /g,'') === "") {
            $scope.nameEditErrorMessage = ResourceUtility.getString('MODEL_TILE_EDIT_TITLE_EMPTY_ERROR');
            $scope.showNameEditError = true;
            $scope.submitting = false;
            return;
        }

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