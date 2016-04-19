angular.module('pd.apiconsole.APIConsoleService', [
    'mainApp.appCommon.utilities.ResourceUtility'
])

.service('APIConsoleService', function ($http, $q, $location, ResourceUtility) {

    this.GetOAuthAccessToken = function (tenantId) {
        var deferred = $q.defer();

        $http({
            method: 'GET',
            url: '/pls/oauth2/accesstoken/json?tenantId=' + tenantId + '.' + tenantId + '.Production',
            headers: {
                'Content-Type': "application/json"
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

        // TODO: cross origin problem
        var result = {
            Success: true,
            ResultObj: getMockupFields(),
            ResultErrors: null
        };
        deferred.resolve(result);
        /*
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
                Success: (data && data.Filelds) ? true : false,
                ResultObj: (data && data.Filelds) ? data.Filelds : null,
                ResultErrors: (data && data.Filelds) ? null : ResourceUtility.getString('API_CONSOLE_SCORING_REQUEST_GET_MODEL_FIELDS_ERROR')
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
        */
        return deferred.promise;
    };

    function getMockupFields() {
        var fields = [];
        var leadId = generateField("LeadId", "Lead ID", "00008875vks3ml9ium");
        fields.push(leadId);
        var email = generateField("Email", "Email", "email@lattice-engines.com");
        fields.push(email);
        var won = generateField("Won", "Won", "True");
        fields.push(won);
        var company = generateField("Company", "Company/Account", "Lattice Engines");
        fields.push(company);
        var city = generateField("City", "City", "SMO");
        fields.push(city);
        var state = generateField("State", "State/Province", "CA");
        fields.push(state);
        var country = generateField("Country", "Country", "USA");
        fields.push(country);
        var createdDate = generateField("CreatedDate", "Created Date", "2016-04-10 16:30:25");
        fields.push(createdDate);
        var lastModifiedDate = generateField("LastModifiedDate", "Last Modified Date", "2016-04-12 15:16:20");
        fields.push(lastModifiedDate);
        var zipCode = generateField("ZipCode", "Zip/Postal Code", "94404");
        fields.push(zipCode);
        var firstName = generateField("FirstName", "First Name", "Tester");
        fields.push(firstName);
        var lastName = generateField("LastName", "Last Name", "External");
        fields.push(lastName);
        var title = generateField("Title", "Title", "Sales");
        fields.push(title);
        var leadSource = generateField("LeadSource", "Lead Source", "Email");
        fields.push(leadSource);
        var closed = generateField("Closed", "Closed", "True");
        fields.push(closed);
        var stage = generateField("Stage", "Stage", "05-Closed Won");
        fields.push(stage);
        return fields;
    }

    function generateField(name, displayName, placeholder) {
        return { name: name, displayName: displayName, placeholder: placeholder };
    }

    this.GetScoreRecord = function (accessToken, scoreRequest) {
        var deferred = $q.defer();

        // TODO: cross origin problem
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
                ResultErrors: ResourceUtility.getString('API_CONSOLE_SCORING_REQUEST_GET_SCORE_RECORD_ERROR')
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