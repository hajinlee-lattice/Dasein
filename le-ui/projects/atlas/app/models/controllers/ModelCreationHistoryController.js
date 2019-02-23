angular.module('mainApp.models.controllers.ModelCreationHistoryController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'common.utilities.browserstorage',
    'mainApp.core.utilities.RightsUtility',
    'mainApp.models.services.ModelService',
    'mainApp.models.modals.ImportModelModal'
])

.controller('ModelCreationHistoryController', function (
    $scope, $rootScope, $compile, BrowserStorageUtility, ResourceUtility, RightsUtility, ModelService
) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.loading = true;

    var clientSession = BrowserStorageUtility.getClientSession();
    if (clientSession == null) { return; }

    $scope.showNoModels = false;
    ModelService.GetAllModels(false).then(function(result) {
        $scope.loading = false;
        if (result != null && result.success === true) {
            var modelList = result.resultObj;
            var contentContainer = $('#modelCreationHistoryContainer');

            var scope = $rootScope.$new();
            scope.data = modelList;
            scope.parentData = modelList;
            scope.metadata = null;

            $compile(contentContainer.html('<div data-model-list-creation-history-widget></div>'))(scope);

        } else if (result.resultErrors === "NO TENANT FOUND") {
            $scope.showNoModels = true;
        }
    });

});