var app = angular.module('mainApp.appCommon.widgets.ModelListCreationHistoryWidget', [
    'mainApp.models.services.ModelService',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.NavUtility',
    'common.services.featureflag',
    'mainApp.models.modals.ImportModelModal'
])
.controller('ModelListCreationHistoryWidgetController', function (
    $scope, $rootScope, $state, ModelService, ResourceUtility,
    FeatureFlagService, NavUtility, ImportModelModal
) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.models = $scope.data;
    var flags = FeatureFlagService.Flags();
    
    $scope.showUploadSummaryJson = FeatureFlagService.FlagIsEnabled(flags.UPLOAD_JSON);

    $scope.showUploadModel = FeatureFlagService.FlagIsEnabled(flags.UPLOAD_JSON);

    $scope.undoDeleteModel = function (modelId) {
        if (modelId == null) {
            return;
        }

        ModelService.undoDeletedModel(modelId).then(function(result){
            if (result.success) {
                $state.go('home.models.history', {}, { reload: true } );
            } else {
                //TODO:song handle errors
                $state.go('home.models.history', {}, { reload: true } );
            }
        });
    };

    $scope.importClicked = function() {
        ImportModelModal.show();
    };

    $scope.importJSON = function() {
        ImportModelModal.show();
    }
})
.directive('modelListCreationHistoryWidget', function () {
    return {
        templateUrl: 'app/AppCommon/widgets/modelListCreationHistoryWidget/ModelListCreationHistoryWidgetTemplate.html'
    };
});
