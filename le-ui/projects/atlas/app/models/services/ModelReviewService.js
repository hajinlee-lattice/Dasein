angular
    .module('lp.models.review', [
        'mainApp.appCommon.utilities.ResourceUtility'
    ])
    .service('ModelReviewStore', function ($q, ModelReviewService) {
        var ModelReviewStore = this;
        this.reviewDataMap = {};

        this.AddDataRule = function (modelId, dataRule) {
            var reviewData = this.reviewDataMap[modelId];
            for (var i in reviewData.dataRules) {
                if (dataRule.name == reviewData.dataRules[i].name) {
                    ModelReviewStore.RemoveDataRule(modelId, dataRule.name);
                }
            }
            reviewData.dataRules.push(dataRule);
        };

        this.GetDataRules = function (modelId) {
            return this.reviewDataMap[modelId].dataRules;
        };

        this.RemoveDataRule = function (modelId, name) {
            var reviewData = this.reviewDataMap[modelId];
            for (var i in reviewData.dataRules) {
                if (reviewData.dataRules[i].name == name) {
                    reviewData.dataRules.splice(i, 1);
                }
            }
        };

        this.SetReviewData = function (modelId, reviewData) {
            this.reviewDataMap[modelId] = reviewData;
        };

        this.GetReviewData = function (modelId, eventTableName) {
            var deferred = $q.defer(),
                reviewData = this.reviewDataMap[modelId];

            if (typeof reviewData == 'object') {
                deferred.resolve(reviewData);
            } else {
                ModelReviewService.GetModelReviewData(modelId, eventTableName).then(function (result) {
                    if (result.Success === true) {
                        var modelReviewData = result.Result;
                        ModelReviewStore.SetReviewData(modelId, modelReviewData);
                        for (var ruleName in modelReviewData.ruleNameToColumnRuleResults) {
                            var foundRule = false;
                            modelReviewData.dataRules.forEach(function (dataRule) {
                                if (dataRule.name == ruleName) {
                                    foundRule = true;
                                }
                            });
                            if (!foundRule || ruleName == "OverlyPredictiveDS") {
                                delete modelReviewData.ruleNameToColumnRuleResults[ruleName];
                                console.log("rule in the column results is not valid, removing column result: " + ruleName);
                            }
                        }
                        deferred.resolve(result.Result);
                    }
                });
            }
            return deferred.promise;
        };

        this.ResetReviewData = function () {
            this.reviewDataMap = {};
        };
    })
    .service('ModelReviewService', function ($q, $http, ResourceUtility, ServiceErrorUtility) {
        this.GetModelReviewData = function (modelId, eventTableName) {
            var deferred = $q.defer();

            $http({
                method: 'GET',
                url: '/pls/models/modelreview/' + modelId,//+ '/' + eventTableName,
                headers: { 'Content-Type': 'application/json' }
            })
                .success(function (data, status, headers, config) {
                    if (data == null || !data.Success) {
                        if (data && data.Errors.length > 0) {
                            var errors = data.Errors.join('\n');
                        }
                        result = {
                            Success: false,
                            ResultErrors: errors || ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR'),
                            Result: null
                        };
                    } else {
                        result = {
                            Success: true,
                            ResultErrors: data.Errors,
                            Result: data.Result
                        };
                    }
                    deferred.resolve(result);
                })
                .error(function (data, status, headers, config) {
                    var result = {
                        Success: false,
                        ResultErrors: data.errorMsg
                    };
                    deferred.resolve(result);
                });

            return deferred.promise;
        };

        this.GetUserUploadedAttributes = function (modelId) {
            var deferred = $q.defer();

            $http({
                method: 'GET',
                url: '/pls/modelsummaries/trainingdata/' + modelId,
                headers: { 'Content-Type': 'application/json' }
            })
                .success(function (data, status, headers, config) {
                    if (data == null || !data.Success) {
                        if (data && data.Errors.length > 0) {
                            var errors = data.Errors.join('\n');
                        }
                        result = {
                            Success: false,
                            ResultErrors: errors || ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR'),
                            Result: null
                        };
                    } else {
                        result = {
                            Success: true,
                            ResultErrors: data.Errors,
                            Result: data.Result
                        };
                    }
                    deferred.resolve(result);
                })
                .error(function (data, status, headers, config) {
                    var result = {
                        Success: false,
                        ResultErrors: data.errorMsg
                    };
                    deferred.resolve(result);
                });

            return deferred.promise;
        };
    });
