angular.module('pd.apiconsole.ScoringRequestController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.BrowserStorageUtility',
    'pd.apiconsole.APIConsoleService'
])

.directive('scoringRequest', function () {
    return {
        templateUrl: 'app/apiConsole/views/ScoringRequestView.html',
        controller: ['$scope', '$stateParams', 'ResourceUtility', 'BrowserStorageUtility', 'APIConsoleService',
                     function ($scope, $stateParams, ResourceUtility, BrowserStorageUtility, APIConsoleService) {
            $scope.ResourceUtility = ResourceUtility;
            initValues();

            $scope.modelChanged = function ($event) {
                if ($event != null) {
                    $event.preventDefault();
                }

                if ($scope.modelId == null || $scope.modelId == '') {
                    initValues();
                } else {
                    $scope.fieldsLoading = true;
                    var token = BrowserStorageUtility.getOAuthAccessToken();
                    if (token != null) {
                        getModelFields(token);
                    } else {
                        APIConsoleService.GetOAuthAccessToken($stateParams.tenantId).then(function (tokenResult) {
                            if (tokenResult.Success) {
                                BrowserStorageUtility.setOAuthAccessToken(tokenResult.ResultObj);
                                getModelFields(tokenResult.ResultObj);
                            } else {
                                handleGetModelFieldsError(tokenResult);
                            }
                        });
                    }
                }
            };

            function initValues() {
                $scope.showFields = false;
                $scope.fieldsLoading = false;
                $scope.showFieldsLoadingError = false;
                $scope.fieldsLoadingError = null;
                $scope.fields = [];
                $scope.scoringRequested = false;
            }

            function getModelFields(token) {
                APIConsoleService.GetModelFields(token, $scope.modelId).then(function (result) {
                    if (result.Success) {
                        $scope.fields = result.ResultObj;
                        $scope.showFields = true;
                        $scope.fieldsLoading = false;
                    } else {
                        handleGetModelFieldsError(result);
                    }
                });
            }

            function handleGetModelFieldsError(result) {
                $scope.showFields = false;
                $scope.showFieldsLoadingError = true;
                $scope.fieldsLoadingError = result.ResultErrors;
                $scope.fieldsLoading = false;
            }

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
                $scope.scoreRecordLoading = true;
                $scope.scoringRequestError = null;
                $scope.score = null;
                $scope.jsonData = null;
                $scope.timeElapsed = null;

                var scoreRequest = { modelId: $scope.modelId, record: {} };
                for (var i = 0; i < $scope.fields.length; i++) {
                    if ($scope.fields[i].value != null) {
                        scoreRequest.record[$scope.fields[i].name] = $scope.fields[i].value;
                    }
                }
                var token = BrowserStorageUtility.getOAuthAccessToken();
                if (token != null) {
                    getScoreRecord(token, scoreRequest);
                } else {
                    APIConsoleService.GetOAuthAccessToken($stateParams.tenantId).then(function (tokenResult) {
                        if (tokenResult.Success) {
                            BrowserStorageUtility.setOAuthAccessToken(tokenResult.ResultObj);
                            getScoreRecord(tokenResult.ResultObj, scoreRequest);
                        } else {
                            $scope.scoringRequestError = result.ResultErrors;
                            $scope.scoreRecordLoading = false;
                        }
                    });
                }
            };

            function getScoreRecord(token, scoreRequest) {
                var start = new Date();
                APIConsoleService.GetScoreRecord(token, scoreRequest).then(function (result) {
                    var end = new Date();
                    $scope.timeElapsed = (end.getTime() - start.getTime()) + ' MS';
                    if (result.ResultObj != null) {
                        $scope.jsonData = JSON.stringify(result.ResultObj, null, "    ");
                    }
                    if (result.Success) {
                        // TODO: init score
                    } else {
                        $scope.scoringRequestError = result.ResultErrors;
                    }
                    $scope.scoreRecordLoading = false;
                });
            }
        }]
    };
});