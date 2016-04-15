angular.module('pd.apiconsole', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.models.services.ModelService',
    'pd.navigation.pagination',
    'pd.apiconsole.ScoringRequestController',
    'pd.apiconsole.ScoringResponseController'
])

.controller('APIConsoleController', function($scope, $state, $stateParams, $rootScope, ResourceUtility, ModelService) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.loading = true;
    ModelService.GetAllModels(true).then(function(result) {
        if (result != null && result.success === true) {
            $scope.models = result.resultObj;
        } else {
            $scope.showLoadingError = true;
            $scope.loadingError = resultErrors;
        }
        $scope.loading = false;
    });
});