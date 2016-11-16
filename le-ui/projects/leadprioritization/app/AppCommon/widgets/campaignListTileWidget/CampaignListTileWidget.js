angular.module('mainApp.appCommon.widgets.CampaignListTileWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.campaigns.services.CampaignService'
])
.directive('campaignListTileWidget', function () {
    return {
        restrict: 'A',
        scope: {
            data:'=',
        },
        templateUrl: 'app/AppCommon/widgets/campaignListTileWidget/CampaignListTileWidgetTemplate.html',
        controller: function(
            $scope, $state, $rootScope, $document, $element, ResourceUtility, DeleteModelModal, CreateCampaignModal
        ) {
            $scope.ResourceUtility = ResourceUtility;
            $scope.nameStatus = {
                editing: false
            };

            var data = $scope.data;
            var flags = FeatureFlagService.Flags();

            $scope.mayChangeModelNames = FeatureFlagService.FlagIsEnabled(flags.CHANGE_MODEL_NAME);
            $scope.mayDeleteModels = FeatureFlagService.FlagIsEnabled(flags.DELETE_MODEL);
            $scope.showRefineAndClone = FeatureFlagService.FlagIsEnabled(flags.REFINE_CLONE);
            $scope.showReviewModel = FeatureFlagService.FlagIsEnabled(flags.REVIEW_MODEL);
            $scope.mayEditModelsClass = $scope.mayChangeModelNames ? "model-name-editable" : "";

            //TODO:pierce Field names subject to change
            $scope.isActive = data.Status === "Active";
            $scope.showCustomMenu = false;

            $scope.customMenuClick = function ($event) {
                if ($event != null) {
                    $event.stopPropagation();
                }
                
                $scope.showCustomMenu = !$scope.showCustomMenu;

                if ($scope.showCustomMenu) {
                    $(document).bind('click', function(event){
                        var isClickedElementChildOfPopup = $element
                            .find(event.target)
                            .length > 0;

                        if (isClickedElementChildOfPopup)
                            return;

                        $scope.$apply(function(){
                            $scope.showCustomMenu = false;
                            $(document).unbind(event);
                        });
                    });
                }
            };

            $scope.refineAndCloneClick = function ($event) {
                if ($event != null) {
                    $event.stopPropagation();
                }
            };

            $scope.reviewClick = function ($event) {
                if ($event != null) {
                    $event.stopPropagation();
                }
            };

            $scope.updateAsActiveClick = function ($event) {

                if ($event != null) {
                    $event.stopPropagation();
                }
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

                $scope.Deactivating = true;

                DeactivateModelModal.show($scope.data.Id);
                $scope.$on('deactivate:modal:cancel', function(event, args) {
                    $scope.Deactivating = false;
                });
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
                if (!$scope.nameStatus.editing && !data.Incomplete) {
                    $rootScope.$broadcast(NavUtility.MODEL_DETAIL_NAV_EVENT, data);
                } else if (!$scope.nameStatus.editing && data.Incomplete && $scope.data.ModelFileType != "PmmlModel") {
                    StaleModelModal.show($scope.data.Id);
                }
            };
            
            $scope.showCopyModelToTenantModal = function($event, model){
                if ($event != null) {
                    $event.stopPropagation();
                }
                CopyModelToTenantModal.show(model);
            }

        }
    };
})
.controller('ChangeCampaignNameController', function (
    $scope, $state, $rootScope, NavUtility, ResourceUtility, ModelStore, ModelService
) {
    $scope.submitting = false;
    $scope.showNameEditError = false;
    $scope.DisplayName = $scope.$parent.data.DisplayName == null ? $scope.$parent.name : $scope.$parent.data.DisplayName;

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

        var validationResult = ModelService.validateModelName($scope.DisplayName);

        if (!validationResult.valid) {
            $scope.nameEditErrorMessage = validationResult.errMsg;
            $scope.showNameEditError = true;
            $scope.submitting = false;
            return;
        }

        ModelService.ChangeModelDisplayName($scope.$parent.data.Id, $scope.DisplayName).then(function(result) {

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
        $scope.DisplayName = $scope.$parent.data.DisplayName == null ? $scope.$parent.name : $scope.$parent.data.DisplayName;
        $scope.nameStatus.editing = false;
    };

    

});