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

    this.GetAllAnalyticPipelines = function () {
        return this.GetAnalyticPipelines();
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
            data: analyticPipeline,
            transformResponse: [function (data, headers, status) {
                // why is this api returning a string!
                try {
                    return JSON.parse(data);
                } catch (e) {
                    if (status === 200) {
                        return {
                            analyticPipelineName: data
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
            url: '/modelquality/analytictests/' + (analyticTestName || ''),
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

    this.GetAnalyticTestByName = function (analyticTestName) {
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
            url: '/modelquality/analytictests/',
            data: analyticTest,
            transformResponse: [function (data, headers, status) {
                // why is this api returning a string!
                try {
                    return JSON.parse(data);
                } catch (e) {
                    if (status === 200) {
                        return {
                            dataSetName: data
                        };
                    } else {
                        return data;
                    }
                }
            }]
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

    this.ExecuteAnalyticTest = function (analyticTestName) {
        var defer = $q.defer();

        var result = {
            success: true,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'PUT',
            url: '/modelquality/analytictests/execute/' + (analyticTestName || ''),
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

    this.CreatePipeline = function (pipelineName, pipelineDescription, pipelineSteps) {
        var defer = $q.defer();

        var result = {
            success: false,
            resultObj: [],
            errMsg: null
        };

        var params = {};
        params.pipelineName = pipelineName;
        if (pipelineDescription) {
            params.pipelineDescription = pipelineDescription;
        }

        $http({
            method: 'POST',
            url: '/modelquality/pipelines/',
            data: pipelineSteps,
            params: params,
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

    // /modelquality/pipelines/pipelinestepfiles/
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

    this.UploadMetadataFile = this.UploadStepFile.bind(this, 'metadata');

    this.UploadPythonFile = this.UploadStepFile.bind(this, 'python');

    // /modelquality/algorithms
    this.GetAlgorithms = function(algorithmName) {
        var defer = $q.defer();

        var result = {
            success: true,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'GET',
            url: '/modelquality/algorithms/' + (algorithmName || '')
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

    this.GetAllAlgorithms = function () {
        return this.GetAlgorithms();
    };

    this.GetAlgorithmByName = function (algorithmName) {
        return this.GetAlgorithms(algorithmName);
    };

    this.LatestAlgorithm = function () {
        var defer = $q.defer();

        var result = {
            success: false,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'POST',
            url: '/modelquality/algorithms/latest'
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

    // /modelquality/dataflows
    this.GetDataflows = function(dataflowName) {
        var defer = $q.defer();

        var result = {
            success: true,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'GET',
            url: '/modelquality/dataflows/' + (dataflowName || '')
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

    this.GetAllDataflows = function () {
        return this.GetDataflows();
    };

    this.GetDataflowByName = function (dataflowName) {
        return this.GetDataflows(dataflowName);
    };

    this.LatestDataflow = function () {
        var defer = $q.defer();

        var result = {
            success: false,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'POST',
            url: '/modelquality/dataflows/latest'
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

    // /modelquality/datasets/
    this.GetDatasets = function (datasetName) {
        var defer = $q.defer();

        var result = {
            success: true,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'GET',
            url: '/modelquality/datasets/' + (datasetName || '')
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

    this.GetAllDatasets = function () {
        return this.GetDatasets();
    };

    this.GetDatasetByName = function (datasetName) {
        return this.GetDatasets(datasetName);
    };

    this.CreateDatasetFromTenant = function (tenantType, tenantId, sourceId) {

        var defer = $q.defer();

        var result = {
            success: false,
            resultObj: [],
            errMsg: null
        };

        var params = {
            tenantType: tenantType,
            tenantId: tenantId,
            sourceId: sourceId
        };

        $http({
            method: 'POST',
            url: '/modelquality/datasets/create',
            params: params,
            transformResponse: [function (data, headers, status) {
                // why is this api returning a string!
                try {
                    return JSON.parse(data);
                } catch (e) {
                    if (status === 200) {
                        return {
                            analyticTestName: data
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

    // /modelquality/propdataconfigs/
    this.GetPropdataConfigs = function (propdataConfigName) {
        var defer = $q.defer();

        var result = {
            success: true,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'GET',
            url: '/modelquality/propdataconfigs/' + (propdataConfigName || '')
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

    this.GetAllPropdataConfigs = function () {
        return this.GetPropdataConfigs();
    };

    this.GetPropdataConfigByName = function (propdataConfigName) {
        return this.GetPropdataConfigs(propdataConfigName);
    };

    this.PropDataLatestForUI = function () {
        var defer = $q.defer();

        var result = {
            success: true,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'POST',
            url: '/modelquality/propdataconfigs/latestForUI'
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

    // /modelquality/samplingconfigs/
    this.GetSamplingConfigs = function(samplingConfigName) {
        var defer = $q.defer();

        var result = {
            success: true,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'GET',
            url: '/modelquality/samplingconfigs/' + (samplingConfigName || '')
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

    this.GetAllSamplingConfigs = function () {
        return this.GetSamplingConfigs();
    };

    this.GetSamplingConfigByName = function (samplingConfigName) {
        return this.GetSamplingConfigs(samplingConfigName);
    };

    this.LatestSamplingConfig = function () {
        var defer = $q.defer();

        var result = {
            success: false,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'POST',
            url: '/modelquality/samplingconfigs/latest'
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

    this.GetAnalyticTestTypes = function () {
        var types = [];
        types.push({name: 'Production'});
        types.push({name: 'Default'});

        return {
            success: true,
            resultObj: types,
            errMsg: null
        };
    };

});
