angular.module('mainApp.appCommon.widgets.ModelListTileWidget', [
    'mainApp.appCommon.utilities.EvergageUtility',
    'mainApp.appCommon.utilities.TrackingConstantsUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.DateTimeFormatUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'mainApp.core.services.FeatureFlagService',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'mainApp.models.services.ModelService',
    'mainApp.models.modals.DeleteModelModal',
    'mainApp.models.modals.StaleModelModal'
])
.controller('ModelListTileWidgetController', function ($scope, $rootScope, $element, ResourceUtility, BrowserStorageUtility, DateTimeFormatUtility,
    EvergageUtility, TrackingConstantsUtility, NavUtility, WidgetFrameworkService, DeleteModelModal, StaleModelModal, FeatureFlagService) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.nameStatus = {
        editing: false
    };
    var widgetConfig = $scope.widgetConfig;
    var data = $scope.data;

    var flags = FeatureFlagService.Flags();
    $scope.mayChangeModelNames = FeatureFlagService.FlagIsEnabled(flags.CHANGE_MODEL_NAME);
    $scope.mayDeleteModels = FeatureFlagService.FlagIsEnabled(flags.DELETE_MODEL);

    $scope.mayEditModelsClass = $scope.mayChangeModelNames ? "model-name-editable" : "";
    if (widgetConfig == null || data == null) {
        return;
    }

    
    //TODO:pierce Field names subject to change
    $scope.displayName = data[widgetConfig.NameProperty];
    $scope.isActive = data[widgetConfig.StatusProperty] === "Active";
    $scope.createdDate = data[widgetConfig.CreatedDateProperty];
    var incomplete = data[widgetConfig.IncompleteProperty];
    
    $scope.modelNameEditClick = function ($event) {
        if ($event != null) {
            $event.stopPropagation();
        }
        
        if ($scope.mayChangeModelNames) {
            //Changing the name of the model
            $scope.nameStatus.editing = true;
        }
    };

    $scope.tileClick = function ($event) {
        if ($event != null && $scope.nameStatus.editing) {
            $event.preventDefault();
        }

        var targetElement = $($event.target);
        if (targetElement.hasClass("fa-trash-o") || targetElement.hasClass("delete-model")) {
            DeleteModelModal.show($scope.data.Id);
        } else if (!$scope.nameStatus.editing && !incomplete) {
            $rootScope.$broadcast(NavUtility.MODEL_DETAIL_NAV_EVENT, data);
        } else if (!$scope.nameStatus.editing && incomplete) {
        	StaleModelModal.show($scope.data.Id);
        }
    };
    
})
.controller('ChangeModelNameController', function ($scope, $rootScope, NavUtility, ResourceUtility, ModelService) {
    $scope.data = {name: $scope.$parent.displayName};
    $scope.submitting = false;
    $scope.showNameEditError = false;
    var displayName = $scope.$parent.displayName;
    console.log(displayName);

    $scope.closeErrorClick = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        $scope.showPasswordError = false;
    };

    $scope.submit = function($event) {
        if ($event != null) $event.stopPropagation();

        $scope.showNameEditError = false;

        if ($scope.submitting) {return;}
        $scope.submitting = true;

        var validationResult = ModelService.validateModelName($scope.data.name);

        if (!validationResult.valid) {
            $scope.nameEditErrorMessage = validationResult.errMsg;
            $scope.showNameEditError = true;
            $scope.submitting = false;
            return;
        }

        ModelService.ChangeModelName($scope.$parent.data.Id, $scope.data.name).then(function(result) {
            if (result.Success) {
                $rootScope.$broadcast(NavUtility.MODEL_LIST_NAV_EVENT, {});
                $scope.nameStatus.editing = false;
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
.directive('modelListTileWidget', function () {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/modelListTileWidget/ModelListTileWidgetTemplate.html'
    };

    return directiveDefinitionObject;
});