angular.module('pd.jobs', [
    'pd.jobs.import.credentials',
    'pd.jobs.import.file',
    'pd.jobs.import.ready',
    'pd.jobs.status'
])

.service('JobsService', function($http, $q, _) {

    var stepsNameDictionary = { "importData": "load_data", "runDataFlow": "load_data",
            "loadHdfsTableToPDServer": "load_data", "match": "match_data", "createEventTableFromMatchResult": "match_data",
            "sample": "generate_insights", "profileAndModel": "create_model" };
    var jobStirng = {"id":4,"name":null,"description":null,"startTimestamp":1449169759000,"endTimestamp":1449171104000,"jobStatus":"Completed","jobType":"fitModelWorkflow","user":null,"steps":[{"name":null,"description":null,"startTimestamp":1449169759000,"endTimestamp":1449170037000,"stepStatus":"Completed","jobStepType":"importData"},{"name":null,"description":null,"startTimestamp":1449170037000,"endTimestamp":1449170153000,"stepStatus":"Completed","jobStepType":"runDataFlow"},{"name":null,"description":null,"startTimestamp":1449170153000,"endTimestamp":1449170184000,"stepStatus":"Completed","jobStepType":"loadHdfsTableToPDServer"},{"name":null,"description":null,"startTimestamp":1449170184000,"endTimestamp":1449170375000,"stepStatus":"Completed","jobStepType":"match"},{"name":null,"description":null,"startTimestamp":1449170375000,"endTimestamp":1449170427000,"stepStatus":"Completed","jobStepType":"createEventTableFromMatchResult"},{"name":null,"description":null,"startTimestamp":1449170427000,"endTimestamp":1449170458000,"stepStatus":"Completed","jobStepType":"sample"},{"name":null,"description":null,"startTimestamp":1449170458000,"endTimestamp":1449171104000,"stepStatus":"Completed","jobStepType":"profileAndModel"}]};
    // var jobStirng = {"id":4,"name":null,"description":null,"startTimestamp":1449169759000,"endTimestamp":1449171104000,"jobStatus":"Pending","jobType":"fitModelWorkflow","user":null,"steps":[]};

    this.GetAllJobs = function() {
        var deferred = $q.defer();
        var result;
        
        /**
        $http({
            method: 'GET',
            url: '/pls/jobs'
        }).then(
            function onSuccess(response) {
                // var jobs = response.data;
                */
        var jobs = [];
        jobs.push(jobStirng);
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
        /**
            }, function onError(response) {

            }
        );
        return deferred.promise;
        */
        return deferred.promise;
    };
    
    this.getJobStatus = function(jobId) {

        var deferred = $q.defer();
        var result;

      /**
        $http({
            method: 'GET',
            url: '/pls/jobs/' + jobId
        }).then(
            function onSuccess(response) { */
                // var jobInfo = response.data;
        var jobInfo = jobStirng;

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
        return deferred.promise;
                /**
            }, function onError(resposne) {
                
            }
        );
                return result;
                */
    };
    
    function getStepRunning(job) {
        if (job.jobStatus != "Running") {
            return null;
        }
        
        for (var i = 0; i < job.steps.length; i++) {
            if (job.steps[i].stepStatus == "Running") {
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
                if (stepsCompleted.indexOf(stepCompleted) == -1) {
                    stepsCompleted.push(stepCompleted);
                }
            }
        }
        
        if (stepsCompleted.indexOf("create_model") > -1) {
            stepsCompleted.push("create_global_target_market");
        }
        return stepsCompleted;
    }
})

.controller('JobsCtrl', function($scope, $rootScope, $http, JobsService) {
    $scope.jobs;
    $scope.statuses = {};
    $scope.showEmptyJobsMessage = false;

    function getAllJobs() {
        console.log("getting all jobs");
        JobsService.GetAllJobs().then(function(result) {
            $scope.jobs = result.resultObj;

            if ($scope.jobs == null || $scope.jobs.length == 0) {
                $scope.showEmptyJobsMessage = true
            } else {
                $scope.showEmptyJobsMessage = false;
            }
        });
    }
    
    var TIME_BETWEEN_JOB_LIST_REFRESH = 5 * 1000;
    var REFRESH_JOBS_LIST_ID;
    
    // REFRESH_JOBS_LIST_ID = setInterval(getAllJobs, TIME_BETWEEN_JOB_LIST_REFRESH);
    
    getAllJobs();
    $scope.$on("$destroy", function() {
        clearInterval(REFRESH_JOBS_LIST_ID);
        $scope.statuses = {};
    });
});
