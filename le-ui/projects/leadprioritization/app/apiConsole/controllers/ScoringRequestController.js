angular.module('pd.apiconsole.ScoringRequestController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.directives.charts.ArcChartDirective',
    'mainApp.core.utilities.BrowserStorageUtility',
    'pd.apiconsole.APIConsoleService'
])

.directive('scoringRequest', function () {
    return {
        templateUrl: 'app/apiConsole/views/ScoringRequestView.html',
        controller: ['$scope', '$q', '$stateParams', '_', 'ResourceUtility', 'BrowserStorageUtility', 'APIConsoleService',
                     function ($scope, $q, $stateParams, _, ResourceUtility, BrowserStorageUtility, APIConsoleService) {
            $scope.ResourceUtility = ResourceUtility;
            $scope.showScoringRequestError = false;
            initValues();

            var displayOrder = ["Email", "CompanyName", "State", "Country", "Website", "FirstName", "LastName"];
            var oldFieldsValuesHash = {};

            $scope.modelChanged = function ($event) {
                if ($event != null) {
                    $event.preventDefault();
                }

                if ($scope.modelId == null || $scope.modelId == '') {
                    initValues();
                } else {
                    $scope.fieldsLoading = true;
                    $scope.showFieldsLoadingError = false;
                    storeOldFieldsValues();
                    $scope.fields = [];
                    // Note: Consider access token will be available in 24 hours, we store it in storage.
                    // If user login one account in different browsers, the saved access token in prior browser will invalid.
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

            function storeOldFieldsValues() {
                oldFieldsValuesHash = {};
                for (var i = 0; i < $scope.fields.length; i++) {
                    if ($scope.fields[i] != null && $scope.fields[i].value != null) {
                        oldFieldsValuesHash[$scope.fields[i].name] = $scope.fields[i].value;
                    }
                }
            }

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
                        shuffleFieldsInOrder(result.ResultObj);
                        $scope.showFields = true;
                        $scope.fieldsLoading = false;
                    } else {
                        handleGetModelFieldsError(result);
                    }
                });
            }

            function shuffleFieldsInOrder(fields) {
                for (var i = 0; i < displayOrder.length; i++) {
                    for (var j = 0; j < fields.length; j++) {
                        if (displayOrder[i] == fields[j].name) {
                            if (fields[j].name != 'FirstName' && fields[j].name != 'LastName') {
                                fields[j].mandatory = true;
                            }
                            fields[j].value = oldFieldsValuesHash[fields[j].name];
                            $scope.fields.push(fields[j]);
                            fields.splice(j, 1);
                        }
                    }
                }

                fields = _.sortBy(fields, 'name');
                for (var i = 0; i < fields.length; i++) {
                    fields[i].value = oldFieldsValuesHash[fields[i].name];
                }
                $scope.fields = $scope.fields.concat(fields);
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
                        if ($scope.fields[i].fieldType.toUpperCase() == 'FLOAT') {
                            scoreRequest.record[$scope.fields[i].name] = Number($scope.fields[i].value);
                        } else {
                            scoreRequest.record[$scope.fields[i].name] = $scope.fields[i].value;
                        }
                    }
                }
                var token = BrowserStorageUtility.getOAuthAccessToken();
                if (token != null) {
                    getScoreRecordWithRetries(token, scoreRequest);
                } else {
                    APIConsoleService.GetOAuthAccessToken($stateParams.tenantId).then(function (tokenResult) {
                        if (tokenResult.Success) {
                            BrowserStorageUtility.setOAuthAccessToken(tokenResult.ResultObj);
                            getScoreRecord(tokenResult.ResultObj, scoreRequest).then(function(scoringResult) {
                                $scope.showScoringRequestError = true;
                                $scope.scoreRecordLoading = false;
                            });
                        } else {
                            $scope.scoringRequestError = result.ResultErrors;
                            $scope.showScoringRequestError = true;
                            $scope.scoreRecordLoading = false;
                        }
                    });
                }
            };

            function refreshAccessToken() {
                var deferred = $q.defer();
                var result = {
                    success: true,
                    accessToken: null
                }

                APIConsoleService.GetOAuthAccessToken($stateParams.tenantId).then(function (tokenResult) {
                    if (tokenResult.Success) {
                        var accessToken = tokenResult.ResultObj;
                        BrowserStorageUtility.setOAuthAccessToken(accessToken);
                        result.accessToken = accessToken;
                        deferred.resolve(result);
                    } else {
                        $scope.scoringRequestError = $scope.scoringRequestError + ' and refresh access token failed';
                        result.success = false;
                        deferred.resolve(result);
                    }
                });

                return deferred.promise;
            }

            function getScoreRecordWithRetries(token, scoringRequest) {
                getScoreRecord(token, scoringRequest).then(function(scoringResult) {
                    if (!scoringResult.success && $scope.scoringRequestError.indexOf('Invalid access token') > -1) {
                        refreshAccessToken().then(function(tokenResult) {
                            if (tokenResult.success) {
                                getScoreRecord(tokenResult.accessToken, scoringRequest).then(function(scoringResult) {
                                    $scope.showScoringRequestError = true;
                                    $scope.scoreRecordLoading = false;
                                });
                            } else {
                                $scope.showScoringRequestError = true;
                                $scope.scoreRecordLoading = false;
                            }
                        });
                    } else {
                        $scope.showScoringRequestError = true;
                        $scope.scoreRecordLoading = false;
                    }
                });
            }

            function getScoreRecord(token, scoreRequest) {
                var deferred = $q.defer();
                var start = new Date();
                var scoringResult = {
                    success: true
                }

                APIConsoleService.GetScoreRecord(token, scoreRequest).then(function (result) {
                    var end = new Date();
                    $scope.timeElapsed = (end.getTime() - start.getTime()) + ' MS';
                    /**
                    if (result.ResultObj != null) {
                        $scope.jsonData = JSON.stringify(result.ResultObj, null, "    ");
                    }
                    */
                    if (result.Success) {
                        $scope.score = result.ResultObj.score;
                        $scope.warnings = result.ResultObj.warnings;
                        $scope.scoreId = result.ResultObj.scoreId;
                        $scope.scoreTimestamp = result.ResultObj.timestamp;

                        $scope.scoringRequestError = null;
                        deferred.resolve(scoringResult);
                    } else {
                        $scope.scoringRequestError = result.ResultErrors;
                        scoringResult.success = false;
                        deferred.resolve(scoringResult);
                    }
                });

                return deferred.promise;
            }

            function stringDivider(str, width, spaceReplacer) {

            }
        }]
    };
});