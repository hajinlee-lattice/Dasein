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
    'mainApp.models.modals.StaleModelModal',
    'mainApp.models.modals.DeactivateModelModal'
])
.controller('ModelListTileWidgetController', function ($http, $scope, $state, $rootScope, $document, $element, ResourceUtility, BrowserStorageUtility, DateTimeFormatUtility,
    EvergageUtility, TrackingConstantsUtility, NavUtility, WidgetFrameworkService, DeleteModelModal, StaleModelModal, DeactivateModelModal, FeatureFlagService, ModelService) {
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
    
    $scope.showCustomMenu = false;
    $scope.customMenuClick = function ($event) {
        if ($event != null) {
            $event.stopPropagation();
        }
        $scope.showCustomMenu = !$scope.showCustomMenu;
    };
    $(document).bind('click', function(event){
        var isClickedElementChildOfPopup = $element
            .find(event.target)
            .length > 0;

        if (isClickedElementChildOfPopup)
            return;

        $scope.$apply(function(){
            $scope.showCustomMenu = false;
        });
    });








    $scope.refineAndCloneClick = function ($event) {
        if ($event != null) {
            $event.stopPropagation();
        }
    };

    $scope.updateAsActiveClick = function ($event) {
        var modelId = $scope.data.Id;

        updateAsActiveModel(modelId);

        function updateAsActiveModel(modelId) {
            ModelService.updateAsActiveModel(modelId).then(function(result) {
                if (result != null && result.success === true) {
                    $state.go('home.models', {}, { reload: true } );
                } else {
                    console.log("errors");
                }
            });
        };
    };

    $scope.updateAsInactiveClick = function ($event) {
        if ($event != null) {
            $event.stopPropagation();
        }
        DeactivateModelModal.show($scope.data.Id);
    };

    $scope.deleteModelClick = function ($event) {
        if ($event != null) {
            $event.stopPropagation();
        }
        DeleteModelModal.show($scope.data.Id);
    };

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

        if (!$scope.nameStatus.editing && !incomplete) {
            $rootScope.$broadcast(NavUtility.MODEL_DETAIL_NAV_EVENT, data);
        } else if (!$scope.nameStatus.editing && incomplete) {
        	StaleModelModal.show($scope.data.Id);
        }
    };
    
})
.controller('ChangeModelNameController', function ($scope, $state, $rootScope, NavUtility, ResourceUtility, ModelStore, ModelService) {
    $scope.submitting = false;
    $scope.showNameEditError = false;
    $scope.displayName = $scope.$parent.displayName == null ? $scope.$parent.name : $scope.$parent.displayName;

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

        var validationResult = ModelService.validateModelName($scope.displayName);

        if (!validationResult.valid) {
            $scope.nameEditErrorMessage = validationResult.errMsg;
            $scope.showNameEditError = true;
            $scope.submitting = false;
            return;
        }

        ModelService.ChangeModelDisplayName($scope.$parent.data.Id, $scope.displayName).then(function(result) {

            if (result.Success) {
                $rootScope.$broadcast(NavUtility.MODEL_LIST_NAV_EVENT, {});

                $scope.nameStatus.editing = false;

                ModelStore.removeModel($scope.$parent.data.Id);

                $state.go('.', {}, { reload: true });

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
        $scope.displayName = $scope.$parent.displayName == null ? $scope.$parent.name : $scope.$parent.displayName;
        $scope.nameStatus.editing = false;
    };

    

})
.directive('modelListTileWidget', function () {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/modelListTileWidget/ModelListTileWidgetTemplate.html'
    };

    return directiveDefinitionObject;
});