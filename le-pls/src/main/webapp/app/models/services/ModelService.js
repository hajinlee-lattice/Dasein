angular.module('mainApp.models.services.ModelService', [
    'mainApp.core.utilities.ServiceErrorUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.UnderscoreUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.core.services.SessionService'
])
.service('ModelService', function ($http, $q, _, ServiceErrorUtility, ResourceUtility, StringUtility, SessionService) {

    this.GetAllModels = function () {
            var deferred = $q.defer();
            var result;

            $http({
                method: 'GET',
                url: '/pls/modelsummaries/',
                headers: {
                    "Content-Type": "application/json"
                }
            })
            .success(function(data, status, headers, config) {
                if (data == null) {
                    result = {
                        success: false,
                        resultObj: null,
                        resultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
                    };
                    deferred.resolve(result);
                } else {
                    result = {
                        success: true,
                        resultObj: null,
                        resultErrors: null
                    };

                    data = _.sortBy(data, 'ConstructionTime').reverse();
                    // sync with front-end json structure
                    result.resultObj = _.map(data, function(rawObj) {
                            return {
                                Id          : rawObj.Id,
                                DisplayName : rawObj.Name,
                                CreatedDate : new Date(rawObj.ConstructionTime).toLocaleDateString(),
                                Status      : rawObj.Active ? 'Active' : 'Inactive'
                            };}
                    );

                }
                deferred.resolve(result);
            })
            .error(function(data, status, headers, config) {
                SessionService.HandleResponseErrors(data, status);
                if (status == 403) {
                    // Users without the privilege of reading models see empty list instead of an error
                    result = {
                        success: true,
                        resultObj: null,
                        resultErrors: null
                    };
                } else {
                    result = {
                        success: false,
                        resultObj: null,
                        resultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
                    };
                }
                deferred.resolve(result);
            });

        return deferred.promise;
    };


    this.GetModelById = function (modelId) {
        var deferred = $q.defer();
        var result;

        $http({
            method: 'GET',
            url: '/pls/modelsummaries/'+ modelId,
            headers: {
                "Content-Type": "application/json"
            }
        })
        .success(function(data, status, headers, config) {
            if (data == null) {
                result = {
                    success: false,
                    resultObj: null,
                    resultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
                };
                deferred.resolve(result);
            } else {
                result = {
                    success: true,
                    resultObj: {},
                    resultErrors: null
                };

                var modelSummary = "";
                if (!StringUtility.IsEmptyString(data.Details.Payload)) {
                    modelSummary = JSON.parse(data.Details.Payload);
                }
                modelSummary.ModelDetails.Active = data.Active;
                // sync with front-end json structure
                result.resultObj = modelSummary;
            }

            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            SessionService.HandleResponseErrors(data, status);
            result = {
                success: false,
                resultObj: null,
                resultErrors: ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR')
            };

            deferred.resolve(result);
        });

        return deferred.promise;
    };

});