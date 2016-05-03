angular.module('pd.jobs', [
    'pd.jobs.import.credentials',
    'pd.jobs.import.file',
    'pd.jobs.import.ready',
    'pd.jobs.status',
    'pd.navigation.pagination',
    'mainApp.core.utilities.BrowserStorageUtility',
])

.service('JobsService', function($http, $q, _, $stateParams) {

    var stepsNameDictionary = { 
        "markReportOutOfDate": "load_data", 
        "importData": "load_data", 
        "createPreMatchEventTable": "match_data",
        "loadHdfsTableToPDServer": "match_data", 
        "match": "match_data", 
        "createEventTableFromMatchResult": "generate_insights", 
        "runImportSummaryDataFlow": "generate_insights",
        "registerImportSummaryReport": "generate_insights", 
        "sample": "generate_insights", 
        "profileAndModel": "create_global_model",
        "chooseModel": "create_global_model",
        "activateModel": "create_global_model",
        "score": "create_global_target_market", 
        "runScoreTableDataFlow": "create_global_target_market", 
        "runAttributeLevelSummaryDataFlows": "create_global_target_market" 
    }; 
    
    var dictionary = {
        'fitModelWorkflow': stepsNameDictionary,/*
        'importMatchAndScoreWorkflow': {
        },*/
        'importMatchAndModelWorkflow': {
            'importData': 'load_data',
            'createEventTableReport': 'load_data',
            'dedupEventTable': 'load_data',
            'createPrematchEventTableReport': 'generate_insights',
            'validatePrematchEventTable': 'generate_insights',
            'matchDataCloud': 'generate_insights',
            'processMatchResult': 'generate_insights',
            'addStandardAttributes': 'create_global_target_market',
            'sample': 'create_global_target_market',
            'exportData': 'create_global_target_market',
            'profileAndModel': 'create_global_target_market',
            'activateModel': 'create_global_target_market'
        },
        'modelAndEmailWorkflow': {
            'sample': 'create_global_target_market',
            'profileAndModel': 'create_global_target_market',
            'activateModel': 'create_global_target_market'
        }
    };

    var numStepsInGroup = { 
        "load_data": 0, 
        "match_data": 0, 
        "generate_insights": 0,
        "create_global_model": 0, 
        "create_global_target_market": 0 
    };

    this.getErrorLog = function(JobReport) {
        var deferred = $q.defer();
        //jobType = jobType == 'importMatchAndModelWorkflow' ? 'models' : 'scores';
        
        $http({
            method: 'GET',
            url: '/pls/fileuploads/' + JobReport.name.replace('_Report','') + '/import/errors',
            headers: { 
                'Accept': 'application/csv;charset=utf-8',
                'ErrorDisplayMethod': 'modal'
            }
        }).then(
            function onSuccess(response) {
                var result = response.data;

                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorCode = response.data.errorCode || 'Error';
                var errorMsg = response.data.errorMsg || 'unspecified error.';

                deferred.reject(errorMsg);
            }
        );

        return deferred.promise;
    }

    this.getScoringResults = function(job) {
        var deferred = $q.defer();
        
        $http({
            method: 'GET',
            url: '/pls/scores/jobs/' + job.id + '/results',
            headers: { 
                'Accept': 'application/csv;charset=utf-8',
                'ErrorDisplayMethod': 'banner'
            }
        }).then(
            function onSuccess(response) {
                var result = response.data;

                deferred.resolve(result);
            }, function onError(response) {
                if (!response.data) {
                    response.data = {};
                }

                var errorCode = response.data.errorCode || 'Error';
                var errorMsg = response.data.errorMsg || 'unspecified error.';

                deferred.reject(errorMsg);
            }
        );

        return deferred.promise;
    }

    this.getAllJobs = function() {
        var deferred = $q.defer();
        var result;
        var modelId = $stateParams.modelId;
        var url = modelId 
            ? '/pls/scores/jobs/' + modelId 
            : '/pls/jobs';

        $http({
            method: 'GET',
            url: url
        }).then(
            function onSuccess(response) {
                var jobs = response.data;
                result = {
                    success: true,
                    resultObj: null
                };
                
                jobs = _.sortBy(jobs, 'startTimestamp');
                result.resultObj = _.map(jobs, function(rawObj) {
                    return {
                        id: rawObj.id,
                        timestamp: rawObj.startTimestamp,
                        jobType: rawObj.jobType,
                        status: rawObj.jobStatus,
                        source: rawObj.inputs.SOURCE_DISPLAY_NAME,
                        user: rawObj.user
                    };
                });
                deferred.resolve(result);
            }, function onError(response) {

            }
        );
        return deferred.promise;
    };
    
    this.getJobStatus = function(jobId) {

        var deferred = $q.defer();
        var result;

        $http({
            method: 'GET',
            url: '/pls/jobs/' + jobId,
            headers: { 
                'ErrorDisplayMethod': 'banner'
            }
        }).then(
            function onSuccess(response) {
                clearNumSteps();
                var jobInfo = response.data;
                var stepRunning = getStepRunning(jobInfo);
                var stepsCompleted = getStepsCompleted(jobInfo);
                var stepFailed = getStepFailed(jobInfo);

                result = {
                    success: true,
                    resultObj:
                        {
                            id: jobInfo.id,
                            user: jobInfo.user,
                            jobType: jobInfo.jobType,
                            jobStatus: jobInfo.jobStatus,
                            startTimestamp: jobInfo.startTimestamp,
                            stepRunning: stepRunning,
                            stepsCompleted: stepsCompleted,
                            stepFailed: stepFailed,
                            completedTimes: getCompletedStepTimes(jobInfo, stepRunning, stepsCompleted),
                            reports: jobInfo.reports
                        }
                };

                deferred.resolve(result);
            }, function onError(response) {
                console.log("getting job failed: " + jobId);
            }
        );
        return deferred.promise;
    };

    this.rescoreTrainingData = function() {
        var deferred = $q.defer();
        var result = {
            success: true
        };
        var modelId = $stateParams.modelId;

        $http({
            method: 'POST',
            url: '/pls/scores/' + modelId + '/training',
            headers: { 
                'ErrorDisplayMethod': 'modal'
            }
        }).then(
            function onSuccess(response) {
                console.log('success',response);
                deferred.resolve(result);
            }, function onError(response) {
                console.log('error',response);
                result.success = false;
                deferred.resolve(result);
            }
        );
        return deferred.promise;
    };
    
    function getCompletedStepTimes(job, runningStep, completedSteps) {
        var completedTimes = { "load_data": null, "match_data": null, "generate_insights": null,
                "create_global_model": null, "create_global_target_market": null };
        var currStepIndex = 0;
        if (runningStep != "load_data" && completedSteps.indexOf("load_data") > -1) {
            currStepIndex += numStepsInGroup.load_data;
            completedTimes.load_data = job.steps[currStepIndex - 1].endTimestamp;
        }
        if (runningStep != "match_data" && completedSteps.indexOf("match_data") > -1) {
            currStepIndex += numStepsInGroup.match_data;
            completedTimes.match_data = job.steps[currStepIndex - 1].endTimestamp;
        }
        if (runningStep != "generate_insights" && completedSteps.indexOf("generate_insights") > -1) {
            currStepIndex += numStepsInGroup.generate_insights;
            completedTimes.generate_insights = job.steps[currStepIndex - 1].endTimestamp;
        }
        if (runningStep != "create_global_model" && completedSteps.indexOf("create_global_model") > -1) {
            currStepIndex += numStepsInGroup.create_global_model;
            completedTimes.create_global_model = job.steps[currStepIndex - 1].endTimestamp;
        }
        if (runningStep != "create_global_target_market" && completedSteps.indexOf("create_global_target_market") > -1) {
            currStepIndex += numStepsInGroup.create_global_target_market;
            completedTimes.create_global_target_market = job.steps[currStepIndex - 1].endTimestamp;
        }
        return completedTimes;
    }

    this.cancelJob = function(jobId) {
        $http({
            method: 'POST',
            url: '/pls/jobs/' + jobId +'/cancel'
        }).then(
            function onSuccess(response) {
                
            }, function onError(response) {
                
            }
        );
    }

    function getStepFailed(job) {
        for (var i = 0; i < job.steps.length; i++) {
            var stepRunning = getDictionaryValue(job, i);

            if (stepRunning && job.steps[i].stepStatus == "Failed") {
                return stepRunning;
            }
        }
        return null;
    }

    function getStepRunning(job) {
        if (job.jobStatus != "Running") {
            //return null;
        }
        if (job.jobType == "modelAndEmailWorkflow") {
            return;
        }
        
        for (var i = 0; i < job.steps.length; i++) {
            var stepRunning = getDictionaryValue(job, i);

            if (stepRunning && job.steps[i].stepStatus == "Running") {
                return stepRunning;
            }
        }
        return null;
    }

    function getDictionaryValue(job, i) {
        var JobType = dictionary[job.jobType];

        var stepDisplayName = JobType
            ? JobType[job.steps[i].jobStepType.trim()]
            : '';

        return stepDisplayName;
    }
    
    function getStepsCompleted(job) {
        if (job.steps == null) {
            return [];
        }
        
        var stepsCompleted = [];
        for (var i = 0; i < job.steps.length; i++) {
            if (job.steps[i].stepStatus == "Completed") {
                var stepCompleted = getDictionaryValue(job, i);

                if (stepCompleted) {
                    numStepsInGroup[stepCompleted] += 1;
                    stepsCompleted.push(stepCompleted);
                }
            }
        }
        
        return stepsCompleted;
    }
    
    function clearNumSteps() {
        numStepsInGroup.load_data = 0;
        numStepsInGroup.match_data = 0;
        numStepsInGroup.generate_insights = 0;
        numStepsInGroup.create_global_model = 0;
        numStepsInGroup.create_global_target_market = 0;
    }
})

.controller('JobsCtrl', function($scope, $state, $stateParams, $http, $timeout, JobsService, BrowserStorageUtility) {
    $scope.jobs;
    $scope.expanded = {};
    $scope.statuses = {};
    $scope.cancelling = {};
    $scope.showEmptyJobsMessage = false;
    $scope.state = $state.current.name == 'home.model.jobs' ? 'model' : 'all';
    //$scope.query = $scope.state == 'model' ? 'importMatchAndModelWorkflow' : '';
    $scope.hideCreationMessage = true;


    function getAllJobs() {
        JobsService.getAllJobs().then(function(result) {
            $scope.jobs = result.resultObj;

            if ($scope.jobs == null || $scope.jobs.length == 0) {
                $scope.showEmptyJobsMessage = true
            } else {
                $scope.showEmptyJobsMessage = false;
            }
        });
    }
    
    var TIME_BETWEEN_JOB_LIST_REFRESH = 20 * 1000;
    var REFRESH_JOBS_LIST_ID;
    
    getAllJobs();
    REFRESH_JOBS_LIST_ID = setInterval(getAllJobs, TIME_BETWEEN_JOB_LIST_REFRESH);

    $scope.$on("JobCompleted", function() {
        $scope.succeeded = true;
    });

    $scope.$on("$destroy", function() {
        clearInterval(REFRESH_JOBS_LIST_ID);
        $scope.expanded = {};
        $scope.statuses = {};
        $timeout.cancel($scope.timeoutTask);
    });

    $scope.handleJobCreationSuccess = function(data) {
        if (data) {
            $scope.jobCreationSuccess = JSON.parse(data);
            $scope.hideCreationMessage = false;
            if ($scope.jobCreationSuccess) {
                if ($scope.state == "all") {
                    $scope.jobQueuedMessage = "Your model has been queued for creation."
                } else if ($scope.jobCreationSuccess == true) {
                    $scope.jobQueuedMessage = "Your scoring job has been queued";
                }
            } else {
                if ($scope.state == "all") {
                    $scope.jobQueuedMessage = "Your model has failed to start running.";
                } else {
                    $scope.jobQueuedMessage = "Your scoring job has failed to start running";
                }
            }

            $scope.timeoutTask = $timeout($scope.closeJobCreationMessage, 30000);
        }
    }

    $scope.handleRescoreClick = function($event) {
        JobsService.rescoreTrainingData().then(function(jobResponse) {
            $scope.handleJobCreationSuccess(jobResponse.success);
        });
        $event.target.disabled = true;
    };
    
    $scope.closeJobSuccessMessage = function() {
        $scope.succeeded = false;
    };

    $scope.closeJobCreationMessage = function() {
        $scope.jobCreationSuccess = null;
        $scope.hideCreationMessage = true;
        if ($scope.state == 'model') {
            $state.go('home.model.jobs', { 'jobCreationSuccess' : null });
        } else {
            $state.go('home.jobs.status', { 'jobCreationSuccess': null });
        }
    };

    $scope.handleJobCreationSuccess($stateParams.jobCreationSuccess);
});
