angular.module('pd.jobs', [
    'pd.jobs.import.credentials',
    'pd.jobs.import.file',
    'pd.jobs.import.ready',
    'pd.jobs.status'
])

.service('JobsService', function($http, $q, _) {

    var stepsNameDictionary = { "markReportOutOfDate": "load_data", "importData": "load_data", "createPreMatchEventTable": "match_data",
            "match": "match_data", "createEventTableFromMatchResult": "generate_insights", "runImportSummaryDataFlow": "generate_insights",
            "registerImportSummaryReport": "generate_insights", "sample": "generate_insights", "profileAndModel": "create_model",
            "chooseModel": "create_model", "score": "create_global_target_market" };

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

                result = {
                    success: true,
                    resultObj:
                        {
                            id: jobInfo.id,
                            user: jobInfo.user,
                            jobType: jobInfo.jobType,
                            jobStatus: jobInfo.jobStatus,
                            stepRunning: getStepRunning(jobInfo),
                            stepsCompleted: getStepsCompleted(jobInfo)
                            // to add step endtimes
                        }
                };

                deferred.resolve(result);
            }, function onError(resposne) {
                
            }
        );
        return deferred.promise;
    };
    
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
            var stepRunning = stepsNameDictionary[job.steps[i].jobStepType];
            if (stepRunning && job.steps[i].stepStatus == "Running") {
                return stepsNameDictionary[job.steps[i].jobStepType];
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
                var stepCompleted = stepsNameDictionary[job.steps[i].jobStepType];
                if (stepCompleted && stepsCompleted.indexOf(stepCompleted) == -1) {
                    stepsCompleted.push(stepCompleted);
                }
            }
        }
        
        return stepsCompleted;
    }
})

.controller('JobsCtrl', function($scope, $rootScope, $http, JobsService) {
    $scope.jobs;
    $scope.expanded = {};
    $scope.statuses = {};
    $scope.showEmptyJobsMessage = false;

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
});
