var app = angular.module("app.modelquality.service.ModelQualityService", [
]);

app.service('ModelQualityService', function($q, $http, $timeout, SessionUtility){

    // /modelquality/analyticpipelines
    this.GetAnalyticPipelines = function(analyticPipelineName) {
        var defer = $q.defer();

        var result = {
            success: true,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'GET',
            url: '/modelquality/analyticpipelines/' + (analyticPipelineName || '')
        }).success(function(data){
            result.resultObj = data;
            defer.resolve(result);
        }).error(function(err, status){
            SessionUtility.handleAJAXError(err, status);
            result.success = false;
            result.errMsg = err;
            defer.reject(result);
        });

        return defer.promise;
    };

    this.GetAnalyticPipelineByName = function (analyticPipelineName) {
        return this.GetAnalyticPipelines(analyticPipelineName);
    };

    this.GetAllAnalyticPipeline = function () {
        return this.GetAnalyticPipelines();
    };

    this.CreateAnalyticPipelineProduction = function (analyticPipeline) {
        var defer = $q.defer();

        var result = {
            success: false,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'POST',
            url: '/modelquality/analyticpipelines/latest/',
            data: analyticPipeline
        }).success(function(data){
            result.success = true;
            defer.resolve(result);
        }).error(function(err, status){
            SessionUtility.handleAJAXError(err, status);
            result.errMsg = err;
            defer.reject(result);
        });

        return defer.promise;
    };

    this.CreateAnalyticPipeline = function (analyticPipeline) {
        var defer = $q.defer();

        var result = {
            success: false,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'POST',
            url: '/modelquality/analyticpipelines/',
            data: analyticPipeline
        }).success(function(data) {
            result.success = true;
            defer.resolve(result);
        }).error(function(err, status){
            SessionUtility.handleAJAXError(err, status);
            result.errMsg = err;
            defer.reject(result);
        });

        return defer.promise;
    };


    // /modelquality/analytictests
    this.GetAnalyticTests = function(analyticTestName) {
        var defer = $q.defer();

        var result = {
            success: true,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'GET',
            url: '/modelquality/analyticpipelines/' + (analyticTestName || ''),
        }).success(function(data){
            result.resultObj = data;
            defer.resolve(result);
        }).error(function(err, status){
            SessionUtility.handleAJAXError(err, status);
            result.success = false;
            result.errMsg = err;
            defer.reject(result);
        });

        return defer.promise;
    };

    this.GetAllAnalyticTests = function () {
        return this.GetAnalyticTests();
    };

    this.GetAnalyticTestsByName = function (analyticTestName) {
        return this.GetAnalyticTests(analyticTestName);
    };

    this.CreateAnalyticTest = function (analyticTest) {
        var defer = $q.defer();

        var result = {
            success: false,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'POST',
            url: '/modelquality/analytictest/',
            data: analyticTest
        }).success(function(data) {
            result.success = true;
            defer.resolve(result);
        }).error(function(err, status){
            SessionUtility.handleAJAXError(err, status);
            result.errMsg = err;
            defer.reject(result);
        });

        return defer.promise;
    };


    // /modelquality/pipelines
    this.GetPipelines = function (pipelineName) {
        var defer = $q.defer();

        var result = {
            success: true,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'GET',
            url: '/modelquality/pipelines/' + (pipelineName || '')
        }).success(function(data){
            result.resultObj = data;
            defer.resolve(result);
        }).error(function(err, status){
            SessionUtility.handleAJAXError(err, status);
            result.success = false;
            result.errMsg = err;
            defer.reject(result);
        });

        return defer.promise;
    };

    this.GetAllPipelines = function () {
        return this.GetPipelines();
    };

    this.GetPipelineByName = function (pipelineName) {
        return this.GetPipelines(pipelineName);
    };

    this.GetPipelinesProuction = function () {
        return this.GetPipelines('latest');
    };

    this.CreatePipeline = function (pipelineName, pipelineSteps) {
        var defer = $q.defer();

        var result = {
            success: false,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'POST',
            url: '/modelquality/pipelines/',
            data: pipelineSteps,
            params: {
                pipelineName: pipelineName
            },
            transformResponse: [function (data, headers, status) {
                // why is this api returning a string!
                try {
                    return JSON.parse(data);
                } catch (e) {
                    if (status === 200) {
                        return {
                            pipelineName: data
                        };
                    } else {
                        return data;
                    }
                }
            }]
        }).success(function(data) {

            result.success = true;
            result.resultObj = data;
            defer.resolve(result);

        }).error(function(err, status){

            SessionUtility.handleAJAXError(err, status);
            result.errMsg = err;
            defer.reject(result);

        });

        return defer.promise;
    };



    this.UploadStepFile = function (type, stepName, fileName, file) {
        var defer = $q.defer();

        var formData = new FormData();
        formData.append('file', file);

        var result = {
            success: false,
            resultObj: [],
            errMsg: null
        };

        var xhr = new XMLHttpRequest();

        xhr.addEventListener('load', function (event) {
            if (xhr.status === 200) {
                result.success = true;
                result.resultObj = {
                    path: xhr.response
                };
                defer.resolve(result);
            } else {
                result.errMsg = JSON.parse(xhr.responseText);
                // resolve instead of reject
                // we want to catch all in $q.all
                defer.resolve(result);
            }
        });

        xhr.addEventListener('error', function (event) {
            result.errMsg = JSON.parse(xhr.responseText);
            defer.resolve(result);
        });

        xhr.open('POST', '/modelquality/pipelines/pipelinestepfiles/' + type + '?stepName=' + stepName + '&fileName=' + fileName);
        xhr.setRequestHeader('Accept', 'application/json');
        xhr.send(formData);

        return defer.promise;
    };

    this.UploadMetadataFile = function (stepName, fileName, file) {
        return this.UploadStepFile('metadata', stepName, fileName, file);
    };

    this.UploadPythonFile = function (stepName, fileName, file) {
        return this.UploadStepFile('python', stepName, fileName, file);
    };

});
