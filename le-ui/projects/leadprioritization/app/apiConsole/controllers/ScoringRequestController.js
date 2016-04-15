angular.module('pd.apiconsole.ScoringRequestController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'pd.apiconsole.APIConsoleService'
])

.directive('scoringRequest', function () {
    return {
        templateUrl: 'app/apiConsole/views/ScoringRequestView.html',
        controller: ['$scope', '$stateParams', 'ResourceUtility', 'APIConsoleService',
                     function ($scope, $stateParams, ResourceUtility, APIConsoleService) {
            $scope.ResourceUtility = ResourceUtility;
            $scope.showFields = false;
            $scope.fieldsLoading = false;
            $scope.showFieldsLoadingError = false;
            $scope.fieldsLoadingError = null;
            $scope.fields = [];

            $scope.modelChanged = function ($event) {
                if ($event != null) {
                    $event.preventDefault();
                }

                if ($scope.modelId == null || $scope.modelId == '') {
                    $scope.showFields = false;
                    $scope.fieldsLoading = false;
                    $scope.showFieldsLoadingError = false;
                    $scope.fields = [];
                    $scope.scoringRequested = false;
                } else {
                    $scope.fieldsLoading = true;
                    APIConsoleService.GetModelFields().then(function (result) {
                        $scope.fields = result;
                        $scope.fieldsLoading = false;
                        $scope.showFields = true;
                    });
                }
            };

            $scope.clearClicked = function ($event) {
                if ($event != null) {
                    $event.preventDefault();
                }

                for (var i = 0; i < $scope.fields.length; i++) {
                    $scope.fields[i].value = null;
                }
            };

            $scope.sentClicked = function ($event) {
                $scope.scoringRequested = true;
                /*APIConsoleService.GetAccessToken($stateParams.tenantId).then(function (accessTokenResult) {
                    var accessToken = null;//accessTokenResult.ResultObj;
                    var scoreRequest = { modelId: $scope.modelId, record: {} };
                    for (var i = 0; i < $scope.fields.length; i++) {
                        if ($scope.fields[i].value != null) {
                            scoreRequest.record[$scope.fields[i].name] = $scope.fields[i].value;
                        }
                    }
                    APIConsoleService.GetScoreRecord(accessToken, scoreRequest).then(function (result) {
                        console.log(result);
                    });
                });*/
            };
        }]
    };
});