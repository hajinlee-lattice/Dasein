angular
.module('lp.models.review', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.service('ModelReviewStore', function($q) {
    var ModelReviewStore = this;
    this.reviewData = {};

    this.GetReviewData = function(modelId) {
        return this.reviewData[modelId];
    }

    this.SetReviewData = function(modelId, reviewData) {
        this.reviewData[modelId] = reviewData;
    }
})
.service('ModelReviewService', function($q, $http, ResourceUtility, ModelReviewStore, ServiceErrorUtility) {
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
