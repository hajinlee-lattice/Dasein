angular.module('lp.apiconsole', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.models.services.ModelService',
    'lp.apiconsole.ScoringRequestController',
    'lp.apiconsole.ScoringResponseController'
])
.controller('APIConsoleController', function($scope, $state, $stateParams, $rootScope, ResourceUtility, ModelService) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.loading = true;
    ModelService.GetAllModels(true).then(function(result) {
        if (result != null && result.success === true) {
            $scope.models = result.resultObj;
            var i = $scope.models.length;
            while (i--) {
                if ($scope.models[i].Status.toLowerCase() != 'active') {
                    $scope.models.splice(i, 1);
                }
            }
        } else {
            $scope.showLoadingError = true;
            $scope.loadingError = resultErrors;
        }
        $scope.loading = false;
    });
});
