var app = angular.module('mainApp.appCommon.widgets.ModelListCreationHistoryWidget', [
    'mainApp.models.services.ModelService',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.GriotNavUtility',
    'mainApp.core.utilities.RightsUtility',
    'mainApp.models.modals.ImportModelModal'
]);

app.controller('ModelListCreationHistoryWidgetController', function ($scope, $rootScope, ModelService, ResourceUtility,
                                                                     RightsUtility, GriotNavUtility, ImportModelModal) {
    
    $scope.ResourceUtility = ResourceUtility;
    $scope.models = $scope.data;
    $scope.showUploadModel = RightsUtility.mayUploadModelJson();

    $scope.undoDeleteModel = function (modelId) {
        if (modelId == null) {
            return;
        }

        ModelService.undoDeletedModel(modelId).then(function(result){
            if (result.Success) {
                $rootScope.$broadcast(GriotNavUtility.MODEL_CREATION_HISTORY_NAV_EVENT);
            } else {
                //TODO:song handle errors
                $rootScope.$broadcast(GriotNavUtility.MODEL_CREATION_HISTORY_NAV_EVENT);
            }
        });
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
