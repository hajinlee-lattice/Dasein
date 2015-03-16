angular.module('mainApp.appCommon.widgets.ModelListCreationHistoryWidget', [
    'mainApp.models.services.ModelService',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.GriotNavUtility',
    'mainApp.models.modals.ImportModelModal'
])
.controller('ModelListCreationHistoryWidgetController', function ($scope, $rootScope, ModelService, ResourceUtility, GriotNavUtility, ImportModelModal) {
    
    $scope.ResourceUtility = ResourceUtility;
    $scope.models = $scope.data;

    $scope.undoDeleteModel = function (modelId) {
        if (modelId == null) {
            return;
        }

        ModelService.undoDeletedModel(modelId);
        $rootScope.$broadcast(GriotNavUtility.MODEL_CREATION_HISTORY_NAV_EVENT);
    };

    $scope.importClicked = function() {
        ImportModelModal.show();
    };
})
.directive('modelListCreationHistoryWidget', function () {
    return {
        templateUrl: 'app/AppCommon/widgets/modelListCreationHistoryWidget/ModelListCreationHistoryWidgetTemplate.html'
    };
});
