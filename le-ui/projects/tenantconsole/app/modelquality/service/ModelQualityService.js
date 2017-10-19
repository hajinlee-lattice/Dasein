angular.module("app.modelquality.service.ModelQualityService", [
])
.service('ModelQualityService', function($q, $http, $timeout, SessionUtility){

    // /modelquality/analyticpipelines
    this.GetAnalyticPipelines = function(analyticPipelineName) {
        var url = '/analyticpipelines/' + (analyticPipelineName || '');
        return this.Get(url);
    };

    this.GetAnalyticPipelineByName = function (analyticPipelineName) {
        return this.GetAnalyticPipelines(analyticPipelineName);
    };

    this.GetAllAnalyticPipelines = function () {
        return this.GetAnalyticPipelines();
    };

    this.CreateAnalyticPipeline = function (analyticPipeline) {
        var url = '/analyticpipelines/';
        return this.Post(url, analyticPipeline, null);
    };

    this.LatestAnalyticPipeline = function () {
        return this.Post('/analyticpipelines/latest', null, null);
    };

    // /modelquality/analytictests
    this.GetAnalyticTests = function(analyticTestName) {
        var url = '/analytictests/' + (analyticTestName || '');
        return this.Get(url);
    };

    this.GetAllAnalyticTests = function () {
        return this.GetAnalyticTests();
    };

    this.GetAnalyticTestByName = function (analyticTestName) {
        return this.GetAnalyticTests(analyticTestName);
    };

    this.CreateAnalyticTest = function (analyticTest) {
        var url = '/analytictests/';
        return this.Post(url, analyticTest, null);
    };

    this.ExecuteAnalyticTest = function (analyticTestName) {
        var url = '/analytictests/execute/' + analyticTestName;
        return this.Put(url, null, null);
    };

    this.UpdateAnalyticTestProduction = function () {
        var url = '/analytictests/updateproduction';
        return this.Put(url, null, null);
    };

    // /modelquality/pipelines
    this.GetPipelines = function (pipelineName) {
        var url = '/pipelines/' + (pipelineName || '');
        return this.Get(url);
    };

    this.GetAllPipelines = function () {
        return this.GetPipelines();
    };

    this.GetPipelineByName = function (pipelineName) {
        return this.GetPipelines(pipelineName);
    };

    this.CreatePipeline = function (pipelineName, pipelineDescription, pipelineSteps) {
        var url = '/pipelines/';
        var data = pipelineSteps;
        var params = {
            pipelineName: pipelineName,
            pipelineDescription: pipelineDescription
        };

        return this.Post(url, data, params);
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

    this.UploadMetadataFile = function (stepName, fileName, file) {
        return this.UploadStepFile('metadata', stepName, fileName, file);
    };

    this.UploadPythonFile = function (stepName, fileName, file) {
        return this.UploadStepFile('python', stepName, fileName, file);
    };

    // /modelquality/datasets/
    this.GetDatasets = function (datasetName) {
        var url = '/datasets/' + (datasetName || '');
        return this.Get(url);
    };

    this.GetAllDatasets = function () {
        return this.GetDatasets();
    };

    this.GetDatasetByName = function (datasetName) {
        return this.GetDatasets(datasetName);
    };

    this.CreateDatasetFromTenant = function (tenantType, tenantId, sourceId) {
        var url = '/datasets/create';
        var params = {
            tenantType: tenantType,
            tenantId: tenantId,
            sourceId: sourceId
        };
        return this.Post(url, null, params);
    };

    // /modelquality/propdataconfigs/
    this.GetPropdataConfigs = function (propdataConfigName) {
        var url = '/propdataconfigs/' + (propdataConfigName || '');
        return this.Get(url);
    };

    this.GetAllPropdataConfigs = function () {
        return this.GetPropdataConfigs();
    };

    this.GetPropdataConfigByName = function (propdataConfigName) {
        return this.GetPropdataConfigs(propdataConfigName);
    };

    this.PropDataLatestForUI = function () {
        var url = '/propdataconfigs/latestForUI';
        return this.Post(url, null, null);
    };

    this.Get = function (url) {
        var defer = $q.defer();

        var result = {
            success: true,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'GET',
            url: '/modelquality'+ url,
            headers: { 'MagicAuthentication': 'Security through obscurity!' }
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

    this.Post = function (url, data, params) {
        var defer = $q.defer();

        var result = {
            success: false,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'POST',
            url: '/modelquality' + url,
            headers: { 'MagicAuthentication': 'Security through obscurity!' },
            data: data,
            params: params,
            transformResponse: [function (data, headers, status) {
                // why is this api returning a string!
                try {
                    return JSON.parse(data);
                } catch (e) {
                    if (status === 200) {
                        return {
                            name: data
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

    this.Put = function (url, data, params) {
        var defer = $q.defer();

        var result = {
            success: true,
            resultObj: [],
            errMsg: null
        };

        $http({
            method: 'PUT',
            url: '/modelquality' + url,
            headers: { 'MagicAuthentication': 'Security through obscurity!' },
            data: data,
            params: params
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

    this.GetAnalyticTestTypes = function () {
        var types = [];
        types.push({name: 'Production', value: 'Production'});
        types.push({name: 'Selected Pipelines', value: 'SelectedPipelines'});

        var result = {
            success: true,
            resultObj: types,
            errMsg: null
        };

        return result;
    };

});
