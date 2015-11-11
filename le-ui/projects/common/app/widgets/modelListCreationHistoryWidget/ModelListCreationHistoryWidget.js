var app = angular.module('mainApp.appCommon.widgets.ModelListCreationHistoryWidget', [
    'mainApp.models.services.ModelService',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.core.services.FeatureFlagService',
    'mainApp.models.modals.ImportModelModal'
]);

app.controller('ModelListCreationHistoryWidgetController', function ($scope, $rootScope, ModelService, ResourceUtility,
                                                                     FeatureFlagService, NavUtility, ImportModelModal) {

    $scope.ResourceUtility = ResourceUtility;
    $scope.models = $scope.data;
    var flags = FeatureFlagService.Flags();
    $scope.showUploadModel = FeatureFlagService.FlagIsEnabled(flags.UPLOAD_JSON);

    $scope.undoDeleteModel = function (modelId) {
        if (modelId == null) {
            return;
        }

        ModelService.undoDeletedModel(modelId).then(function(result){
            if (result.Success) {
                $rootScope.$broadcast(NavUtility.MODEL_CREATION_HISTORY_NAV_EVENT);
            } else {
                //TODO:song handle errors
                $rootScope.$broadcast(NavUtility.MODEL_CREATION_HISTORY_NAV_EVENT);
            }
        });
    };

    $scope.importClicked = function() {
        ImportModelModal.show();
    };
})
    .directive('modelListCreationHistoryWidget', function () {
        return {
            templateUrl: '/app/widgets/modelListCreationHistoryWidget/ModelListCreationHistoryWidgetTemplate.html'
        };
    });
