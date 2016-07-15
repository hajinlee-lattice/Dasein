angular
.module('lp.models.review', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.service('ModelReviewStore', function($q, ModelReviewService) {
    var ModelReviewStore = this;
    this.reviewDataMap = {};

    this.AddDataRule = function(modelId, dataRule) {
        var reviewData = this.reviewDataMap[modelId];
        for (var i in reviewData.dataRules) {
            if (dataRule.name == reviewData.dataRules[i].name) {
                reviewData.dataRules.splice(i, 1);
            }
        }
        reviewData.dataRules.push(dataRule);
        this.reviewDataMap[modelId] = reviewData;
    };

    this.GetDataRules = function(modelId) {
        return this.reviewDataMap[modelId].dataRules;
    };

    this.RemoveDataRule = function(modelId, name) {
        var reviewData = this.reviewDataMap[modelId];
        for (var i in reviewData.dataRules) {
            if (reviewData.dataRules[i].name == name) {
                reviewData.dataRules.splice(i, 1);
            }
        }
        this.reviewDataMap[modelId] = reviewData;
    };

    this.SetReviewData = function(modelId, reviewData) {
        this.reviewDataMap[modelId] = reviewData;
    };

    this.GetReviewData = function(modelId, eventTableName) {
        var deferred = $q.defer(),
            reviewData = this.reviewDataMap[modelId];

        if (typeof reviewData == 'object') {
            deferred.resolve(reviewData);
        } else {
             ModelReviewService.GetModelReviewData(modelId, eventTableName).then(function(result) {
                 if (result.Success === true) {
                     var modelReviewData = result.Result;
                     ModelReviewStore.SetReviewData(modelId, modelReviewData);
                     for (var i in modelReviewData.dataRules) {
                        ModelReviewStore.AddDataRule(modelId, modelReviewData.dataRules[i]);
                     }
                     deferred.resolve(result.Result);
                 }
             });

            return deferred.promise;
        }
    };

    this.ResetReviewData = function() {
        this.reviewDataMap = {};
    };
})
.service('ModelReviewService', function($q, $http, ResourceUtility, ServiceErrorUtility) {
    this.GetModelReviewData = function(modelId, eventTableName) {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/models/reviewmodel/mocked/' + modelId + '/' + eventTableName,
            headers: { 'Content-Type': 'application/json' }
        })
        .success(function(data, status, headers, config) {
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
        .error(function(data, status, headers, config) {
            var result = {
                Success: false,
                ResultErrors: data.errorMsg
            };
            deferred.resolve(result);
        });

        return deferred.promise;
    };
});
