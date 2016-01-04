angular.module('pd.jobs', [
    'pd.jobs.import.credentials',
    'pd.jobs.import.file',
    'pd.jobs.import.ready',
    'pd.jobs.status',
    'pd.navigation.pagination',
    'mainApp.core.utilities.BrowserStorageUtility',
])

.service('JobsService', function($http, $q, _) {

    var stepsNameDictionary = { "markReportOutOfDate": "load_data", "importData": "load_data", "createPreMatchEventTable": "match_data",
            "loadHdfsTableToPDServer": "match_data", "match": "match_data", "createEventTableFromMatchResult": "generate_insights", "runImportSummaryDataFlow": "generate_insights",
            "registerImportSummaryReport": "generate_insights", "sample": "generate_insights", "profileAndModel": "create_global_model",
            "chooseModel": "create_global_model", "score": "create_global_target_market" };
    var numStepsInGroup = { "load_data": 1, "match_data": 1, "generate_insights": 1, "create_global_model": 1, "create_global_target_market": 1 };

    this.getAllJobs = function() {
        var deferred = $q.defer();
        var result;
        
        $http({
            method: 'GET',
            url: '/pls/jobs'
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
            url: '/pls/jobs/' + jobId
        }).then(
            function onSuccess(response) {
                var jobInfo = response.data;
                var stepRunning = getStepRunning(jobInfo);
                var stepsCompleted = getStepsCompleted(jobInfo);

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
                            completedTimes: getCompletedStepTimes(jobInfo, stepRunning, stepsCompleted)
                        }
                };

                deferred.resolve(result);
            }, function onError(resposne) {
                
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
            method: 'GET',
            url: '/pls/jobs/' + jobId +'/cancel'
        }).then(
            function onSuccess(response) {
                
            }, function onError(response) {
                
            }
        );
    }
    
    function getStepRunning(job) {
        if (job.jobStatus != "Running") {
            return null;
        }
        
        for (var i = 0; i < job.steps.length; i++) {
            var stepRunning = stepsNameDictionary[job.steps[i].jobStepType.trim()];
            if (stepRunning && job.steps[i].stepStatus == "Running") {
                return stepsNameDictionary[job.steps[i].jobStepType.trim()];
            }
        }
        return null;
    }
    
    function getStepsCompleted(job) {
        if (job.steps == null) {
            return [];
        }
        
        var stepsCompleted = [];
        for (var i = 0; i < job.steps.length; i++) {
            if (job.steps[i].stepStatus == "Completed") {
                var stepCompleted = stepsNameDictionary[job.steps[i].jobStepType.trim()];
                if (stepCompleted) {
                    stepsCompleted.push(stepCompleted);
                }
            }
        }
        
        return stepsCompleted;
    }
})

.controller('JobsCtrl', function($scope, $rootScope, $http, JobsService, BrowserStorageUtility) {
    $scope.jobs;
    $scope.expanded = {};
    $scope.statuses = {};
    $scope.showEmptyJobsMessage = false;
    $scope.showJobSuccessMessage = BrowserStorageUtility.getSessionShouldShowJobCompleteMessage();

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
    
    var TIME_BETWEEN_JOB_LIST_REFRESH = 45 * 1000;
    var REFRESH_JOBS_LIST_ID;
    
    getAllJobs();
    REFRESH_JOBS_LIST_ID = setInterval(getAllJobs, TIME_BETWEEN_JOB_LIST_REFRESH);
    
    $scope.$on("$destroy", function() {
        clearInterval(REFRESH_JOBS_LIST_ID);
        $scope.expanded = {};
        $scope.statuses = {};
    });
    
    $scope.closeJobSuccessMessage = function() {
        $scope.showJobSuccessMessage = false;
        BrowserStorageUtility.setSessionShouldShowJobCompleteMessage(false);
    };
});
