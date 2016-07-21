angular.module('pd.jobs', [
    'pd.jobs.import.credentials',
    'pd.jobs.import.file',
    'pd.jobs.import.ready',
    'pd.jobs.status',
    'pd.navigation.pagination',
    'mainApp.models.leadenrichment',
    'mainApp.core.utilities.BrowserStorageUtility'
])
.run(function($interval, JobsStore) {
    JobsStore.getJobs();

    $interval(function() {
        JobsStore.getJobs();
    }, 30 * 1000);
})
.service('JobsStore', function($q, JobsService) {
    var JobsStore = this;

    this.data = {
        jobs: [],
        models: {},
        jobsMap: {}
    };

    this.getJob = function(jobId) {
        var deferred = $q.defer(),
            job = this.data.jobsMap[jobId];
        
        if (typeof job == 'object') {
            deferred.resolve(job);
        } else {
            JobsService.getJobStatus(jobId).then(function(response) {
                deferred.resolve(response.resultObj);
            });
        }

        return deferred.promise;
    };

    this.getJobs = function(use_cache, modelId) {
        var deferred = $q.defer(),
            jobs = modelId 
                ? this.data.models[modelId] 
                : this.data.jobs;

        if (use_cache) {
            if (jobs && jobs.length > 0) {
                deferred.resolve(jobs);
            } else {
                this.data.models[modelId] = [];
                deferred.resolve([]);
            }
        } else {
            JobsService.getAllJobs().then(function(response) {
                var response = response.resultObj;

                if (modelId) {
                    JobsStore.data.models[modelId].length = 0;
                } else {
                    JobsStore.data.jobs.length = 0;
                }
                
                for (var i=0; i<response.length; i++) {
                    var job = response[i];

                    JobsStore.addJobMap(job.id, job);
                    JobsStore.addJob(job, modelId);
                }

                deferred.resolve(JobsStore.data.jobs);
            });
        }

        return deferred.promise;
    };

    this.addJob = function(job, modelId) {
        if (modelId) {
            JobsStore.data.models[modelId].push(job);
        } else {
            JobsStore.data.jobs.push(job);
        }
    };

    this.addJobMap = function(jobId, job) {
        this.data.jobsMap[jobId] = job;
    };

    this.removeJob = function(jobId) {
        delete this.data.jobsMap[jobId];
    };
})
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
            'addStandardAttributesViaJavaFunction': 'create_global_target_market',
            'sample': 'create_global_target_market',
            'exportData': 'create_global_target_market',
            'setMatchSelection': 'create_global_target_market',
            'writeMetadataFiles': 'create_global_target_market',
            'profile': 'create_global_target_market',
            'reviewModel': 'create_global_target_market',
            'remediateDataRules': 'create_global_target_market',
            'cleanInitialReview': 'create_global_target_market',
            'sample': 'create_global_target_market',
            'exportData': 'create_global_target_market',
            'setMatchSelection': 'create_global_target_market',
            'writeMetadataFiles': 'create_global_target_market',
            'profile': 'create_global_target_market',
            'reviewModel': 'create_global_target_market',
            'createModel': 'create_global_target_market',
            'downloadAndProcessModelSummaries': 'create_global_target_market'
        },
        'modelAndEmailWorkflow': {
            'dedupEventTable': 'create_global_target_market',
            'matchDataCloud': 'create_global_target_market',
            'processMatchResult': 'create_global_target_market',
            'addStandardAttributesViaJavaFunction': 'create_global_target_market',
            'remediateDataRules': 'create_global_target_market',
            'resolveMetadataFromUserRefinedAttributes': 'create_global_target_market',
            'sample': 'create_global_target_market',
            'exportData': 'create_global_target_market',
            'setMatchSelection': 'create_global_target_market',
            'writeMetadataFiles': 'create_global_target_market',
            'profile': 'create_global_target_market',
            'reviewModel': 'create_global_target_market',
            'createModel': 'create_global_target_market',
            'downloadAndProcessModelSummaries': 'create_global_target_market'
        },
        'pmmlModelWorkflow': {
            'createPMMLModel': 'create_global_target_market'
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
            url: url,
            headers: {
                'If-Modified-Since': 0,
                'ErrorDisplayMethod': 'none'
            }
        }).then(
            function onSuccess(response) {
                var jobs = response.data;
                result = {
                    success: true,
                    resultObj: null
                };
                
                jobs = _.sortBy(jobs, 'startTimestamp');
                result.resultObj = _.map(jobs, function(job) {
                    clearNumSteps();
                    var stepRunning = getStepRunning(job);
                    var stepsCompleted = getStepsCompleted(job);
                    var stepFailed = getStepFailed(job);
                    
                    return {
                        id: job.id,
                        timestamp: job.startTimestamp,
                        errorCode: job.errorCode,
                        errorMsg: job.errorMsg,
                        jobType: job.jobType,
                        status: job.jobStatus,
                        source: job.inputs.SOURCE_DISPLAY_NAME,
                        user: job.user,
                        jobStatus: job.jobStatus,
                        startTimestamp: job.startTimestamp,
                        stepRunning: stepRunning,
                        stepsCompleted: stepsCompleted,
                        stepFailed: stepFailed,
                        completedTimes: getCompletedStepTimes(job, stepRunning, stepsCompleted),
                        reports: job.reports
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
                'ErrorDisplayMethod': 'banner',
                'If-Modified-Since': 0
            }
        }).then(
            function onSuccess(response) {
                clearNumSteps();
                var job = response.data;
                var stepRunning = getStepRunning(job);
                var stepsCompleted = getStepsCompleted(job);
                var stepFailed = getStepFailed(job);

                result = {
                    success: true,
                    resultObj:
                        {
                            id: job.id,
                            user: job.user,
                            errorCode: job.errorCode,
                            errorMsg: job.errorMsg,
                            jobType: job.jobType,
                            jobStatus: job.jobStatus,
                            startTimestamp: job.startTimestamp,
                            stepRunning: stepRunning,
                            stepsCompleted: stepsCompleted,
                            stepFailed: stepFailed,
                            completedTimes: getCompletedStepTimes(job, stepRunning, stepsCompleted),
                            reports: job.reports
                        }
                };

                deferred.resolve(result);
            }
        );
        return deferred.promise;
    };

    this.getJobStatusFromApplicationId = function(applicationId) {

        var deferred = $q.defer();
        var result;

        $http({
            method: 'GET',
            url: '/pls/jobs/yarnapps/' + applicationId,
            headers: {
                'ErrorDisplayMethod': 'banner',
                'If-Modified-Since': 0
            }
        }).then(
            function onSuccess(response) {
                clearNumSteps();
                var job = response.data;
                var stepRunning = getStepRunning(job);
                var stepsCompleted = getStepsCompleted(job);
                var stepFailed = getStepFailed(job);

                result = {
                    success: true,
                    resultObj: {
                        id: job.id,
                        user: job.user,
                        errorCode: job.errorCode,
                        errorMsg: job.errorMsg,
                        jobType: job.jobType,
                        jobStatus: job.jobStatus,
                        startTimestamp: job.startTimestamp,
                        stepRunning: stepRunning,
                        stepsCompleted: stepsCompleted,
                        stepFailed: stepFailed,
                        completedTimes: getCompletedStepTimes(job, stepRunning, stepsCompleted),
                        reports: job.reports
                    }
                };
                deferred.resolve(result);
            },
            function onError(response) {
                result = {
                    success: false,
                    resultObj: {
                        id: job.id,
                        user: job.user,
                        errorCode: job.errorCode,
                        errorMsg: job.errorMsg,
                        jobType: job.jobType,
                    }
                };
                deferred.resolve(result);
            }
        );

        return deferred.promise;
    };

    this.rescoreTrainingData = function(performEnrichment) {
        var deferred = $q.defer();
        var result = {
            success: true
        };
        var modelId = $stateParams.modelId;

        $http({
            method: 'POST',
            url: '/pls/scores/' + modelId + '/training',
            params: {
                'performEnrichment': performEnrichment,
                'useRtsApi': performEnrichment
            },
            headers: { 
                'ErrorDisplayMethod': 'modal'
            }
        }).then(
            function onSuccess(response) {
                deferred.resolve(result);
            }, function onError(response) {
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
        if (job.steps) {
            for (var i = 0; i < job.steps.length; i++) {
                var stepRunning = getDictionaryValue(job, i);
                if (stepRunning && (job.steps[i].stepStatus == "Failed" || job.steps[i].stepStatus == "Cancelled")) {
                    return stepRunning;
                }
            }
        }
        return null;
    }

    function getStepRunning(job) {
        if (job.jobStatus != "Running") {
            //return null;
        }

        if (!job.steps || job.jobType == "modelAndEmailWorkflow") {
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

.controller('JobsCtrl', function($scope, $state, $stateParams, $http, $timeout, $interval, JobsStore, JobsService, BrowserStorageUtility, ScoreLeadEnrichmentModal) {
    $scope.expanded = {};
    $scope.statuses = {};
    $scope.cancelling = {};
    $scope.showEmptyJobsMessage = false;
    $scope.hideCreationMessage = true;
    $scope.state = $state.current.name == 'home.model.jobs' ? 'model' : 'all';
    $scope.jobs = [];
    
    var modelId = $scope.state == 'model' ? $stateParams.modelId : null;

    if (modelId) {
        if (!JobsStore.data.models[modelId]) {
            JobsStore.data.models[modelId] = [];
        }

        $scope.jobs = JobsStore.data.models[modelId];
    } else {

        
        $timeout(function() {
            $scope.jobs = JobsStore.data.jobs;
        },100);
    }

    function getAllJobs(use_cache) {
        JobsStore.getJobs(use_cache, modelId).then(function(result) {
            $scope.showEmptyJobsMessage = (($scope.jobs == null || $scope.jobs.length == 0) && !use_cache);
        });
    }
    
    var BULK_SCORING_INTERVAL = 30 * 1000,
        BULK_SCORING_ID;

    // this stuff happens only on Model Bulk Scoring page
    if (modelId) {
        getAllJobs();
        BULK_SCORING_ID = $interval(getAllJobs, BULK_SCORING_INTERVAL);
    }

    $scope.$on("JobCompleted", function() {
        $scope.succeeded = true;
        if ($scope.state == 'model') {
            $scope.successMsg = 'Success! Scoring job has completed.';
        } else {
            $scope.successMsg = 'Success! Modeling job has completed.';
        }
    });

    $scope.$on("$destroy", function() {
        $interval.cancel(BULK_SCORING_ID);
        $scope.expanded = {};
        $scope.statuses = {};
        $timeout.cancel($scope.timeoutTask);
    });

    $scope.$on("SCORING_JOB_SUCCESS", function(event, data) {
        $scope.handleJobCreationSuccess(data);
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
        $event.target.disabled = true;
        ScoreLeadEnrichmentModal.showRescoreModal();
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
