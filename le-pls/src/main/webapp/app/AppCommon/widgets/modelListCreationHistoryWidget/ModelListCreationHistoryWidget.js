angular.module('mainApp.appCommon.widgets.ModelListCreationHistoryWidget', [
    'mainApp.models.services.ModelService',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.GriotNavUtility',
    
])
.controller('ModelListCreationHistoryWidgetController', function ($scope, $rootScope, ModelService, ResourceUtility, GriotNavUtility) {
    
    $scope.ResourceUtility = ResourceUtility;
    $scope.models = $scope.data;
    
    $scope.undoDeleteModel = function (modelId) {
        if (modelId == null) {
            return;
        }
        
        ModelService.undoDeletedModel(modelId);
        $rootScope.$broadcast(GriotNavUtility.MODEL_CREATION_HISTORY_NAV_EVENT);
    };
    
})
.directive('modelListCreationHistoryWidget', function ($compile) {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/modelListCreationHistoryWidget/ModelListCreationHistoryWidgetTemplate.html'
    };

    return directiveDefinitionObject;
});