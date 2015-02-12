angular.module('mainApp.models.services.GriotModelService', [
    'mainApp.core.utilities.ServiceErrorUtility',
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.UnderscoreUtility'
])
.service('GriotModelService', function ($http, $q, _, ServiceErrorUtility, ResourceUtility) {

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
                        DisplayName: rawObj.Name,
                        CreatedDate: new Date(rawObj.ConstructionTime).toLocaleDateString(),
                        TestSet: rawObj.TestRowCount,
                        TotalLeads: rawObj.TotalRowCount,
                        TotalSuccessEvents: rawObj.TotalConversionCount,
                        TrainingSet: rawObj.TrainingRowCount,
                        ConversionRate: rawObj.TotalConversionCount/rawObj.TotalRowCount,
                        Status: "Active"    //TODO:[2015Feb11] read Status from DB
                        //TODO:[2015Feb11] missing fields: ExternalAttributes, InternalAttributes, Opportunity, LeadSource
                    };}
                );
            }
            deferred.resolve(result);
        })
        .error(function(data, status, headers, config) {
            if (status == 403) {
                //users without the privilege of reading models see empty list instead of an error
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

});