angular.module('pd.apiconsole.APIConsoleService', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.appCommon.utilities.AnimationUtility'
])

.service('APIConsoleService', function ($http, $q, $location, ResourceUtility, StringUtility, AnimationUtility) {

    this.GetOAuthAccessToken = function (tenantId) {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/oauth2/accesstoken/json?tenantId=' + tenantId + '.' + tenantId + '.Production',
            headers: {
                'Content-Type': "application/json",
                'ErrorDisplayMethod': 'modal|home.models'
            }
        }).success(function (data, status, headers, config) {
            var result = {
                Success: (data && data.token) ? true : false,
                ResultObj: (data && data.token) ? data.token : null,
                ResultErrors: (data && data.token) ? null : ResourceUtility.getString('API_CONSOLE_SCORING_REQUEST_GET_ACCESS_TOKEN_ERROR')
            };
            deferred.resolve(result);
        })
        .error(function (data, status, headers, config) {
            var result = {
                Success: false,
                ResultObj: null,
                ResultErrors: ResourceUtility.getString('API_CONSOLE_SCORING_REQUEST_GET_ACCESS_TOKEN_ERROR')
            };
            deferred.resolve(result);
        });

        return deferred.promise;
    };


    this.GetModelFields = function (accessToken, modelId) {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: getScoringApiUrl() + '/models/' + modelId + '/fields',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + accessToken
            }
        })
        .success(function (data, status, headers, config) {
            var result = {
                Success: (data && data.fields) ? true : false,
                ResultObj: (data && data.fields) ? getDisplayFieldsFromResponseFields(data.fields) : null,
                ResultErrors: (data && data.fields) ? null : ResourceUtility.getString('API_CONSOLE_SCORING_REQUEST_GET_MODEL_FIELDS_ERROR')
            };
            deferred.resolve(result);
        })
        .error(function (data, status, headers, config) {
            var result = {
                Success: false,
                ResultObj: null,
                ResultErrors: ResourceUtility.getString('API_CONSOLE_SCORING_REQUEST_GET_MODEL_FIELDS_ERROR')
            };
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    this.CalculateArcColor = function (score) {
        if (score == null) {
            return null;
        }

        var highestRgb = {R:27, G:172, B:94};
        var midRangeRgb = {R:255, G:255, B:102};
        var lowestRgb = {R:178, G:0, B:0};

        var rgbColor = null;
        if (score > 50) {
            rgbColor = AnimationUtility.CalculateRgbBetweenValues(highestRgb, midRangeRgb, score);
            return AnimationUtility.ConvertRgbToHex(rgbColor.R, rgbColor.G, rgbColor.B);
        } else {
            // Need to double the score when it is below 50
            // because we use a different color scale from 0-50
            score = score * 2;
            rgbColor = AnimationUtility.CalculateRgbBetweenValues(midRangeRgb, lowestRgb, score);
            return AnimationUtility.ConvertRgbToHex(rgbColor.R, rgbColor.G, rgbColor.B);
        }
    };

    function getDisplayFieldsFromResponseFields(responseFields) {
        var displayFields = [];

        for (var i = 0; i < responseFields.length; i++) {
            displayFields.push(generateField(responseFields[i]));
        }

        return displayFields;
    }

    function generateField(field) {
        return { name : field.fieldName, displayName: StringUtility.SubstituteAllSpecialCharsWithSpaces(field.fieldName), placeholder: field.fieldValue, fieldType: field.fieldType };
    }

    this.GetScoreRecord = function (accessToken, scoreRequest) {
        var deferred = $q.defer();

        $http({
            method: 'POST',
            url: getScoringApiUrl() + '/record',
            data: scoreRequest,
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + accessToken
            }
        })
        .success(function (data, status, headers, config) {
            var result = {
                Success: data ? true : false,
                ResultObj: data,
                ResultErrors: data ? null : ResourceUtility.getString('API_CONSOLE_SCORING_REQUEST_GET_SCORE_RECORD_ERROR')
            };
            deferred.resolve(result);
        })
        .error(function (data, status, headers, config) {
            var jsonData = null;
            if (typeof data === 'string') {
                jsonData = { error: data, code: status };
            } else if (data != null) {
                jsonData = data;
                jsonData.code = status;
            }
            var result = {
                Success: false,
                ResultObj: jsonData,
                ResultErrors: data.error_description
            };
            deferred.resolve(result);
        });

        return deferred.promise;
    };

    function getScoringApiUrl() {
        var appUrl = $location.protocol() + '://' + $location.host();
        var port = $location.port();
        if (port != 80) {
            appUrl += ":" + port;
        }

        // e.g. http://app.lattice.local --> http://api.lattice.local
        var apiUrl = appUrl.replace(/app/i, 'api');
        if (apiUrl.charAt(apiUrl.length - 1) ===  '/') {
            apiUrl += 'score';
        } else {
            apiUrl += '/score';
        }

        return apiUrl;
    }

});