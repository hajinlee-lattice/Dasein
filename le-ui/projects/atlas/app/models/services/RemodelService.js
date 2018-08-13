angular
.module('lp.models.remodel', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.service('RemodelStore', function($q, RemodelService) {
    var RemodelStore = this;
    this.remodelAttributesMap = {};
    this.remodelDataRulesMap = {};

    this.GetModelReviewAttributes = function(modelId) {
        var deferred = $q.defer(),
            attributes = this.remodelAttributesMap[modelId];

        if (typeof attributes === 'object') {
            deferred.resolve(attributes);
        } else {
            RemodelService.GetModelReviewAttributes(modelId).then(function(result) {
                if (result.Success === true) {
                    RemodelStore.remodelAttributesMap[modelId] = result.Result;
                    deferred.resolve(result.Result);
                } else {
                    deferred.resolve(result.ResultErrors);
                }
            });
        }

        return deferred.promise;
    };

    this.GetModelReviewDataRules = function(modelId) {
        var deferred = $q.defer(),
            dataRules = this.remodelDataRulesMap[modelId];

        if (typeof dataRules === 'object') {
            deferred.resolve(dataRules);
        } else {
            RemodelService.GetModelReviewDataRules(modelId).then(function(result) {
                if (result.Success === true) {
                    RemodelStore.remodelDataRulesMap[modelId] = result.Result;
                    deferred.resolve(result.Result);
                } else {
                    deferred.reject(result.ResultErrors);
                }
            });
        }

        return deferred.promise;
    };

    this.ResetRemodelData = function() {
        this.remodelAttributesMap = {};
        this.remodelDataRulesMap = {};
    };
})
.service('RemodelService', function($q, $http, ResourceUtility, ServiceErrorUtility) {

    this.GetModelReviewDataRules = function(modelId) {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/models/modelreview/' + modelId,
            headers: { 'Content-Type': 'application/json' }
        })
        .success(function(data, status, headers, config) {
            var result;
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

    this.GetModelReviewAttributes = function(modelId) {
        var deferred = $q.defer();
        $http({
            method: 'GET',
            url: '/pls/models/modelreview/attributes/' + modelId,
            headers: { 'Content-Type': 'application/json' }
        })
        .success(function(data, status, headers, config) {
            var result;
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
