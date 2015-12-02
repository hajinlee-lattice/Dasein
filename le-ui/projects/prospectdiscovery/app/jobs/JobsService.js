angular.module('services.jobs', [
])

.service('JobsService', function($http, $q, _) {
    this.GetAllJobs = function() {
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
                        jobtype: rawObj.jobType,
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
            url: '/pls/job/' + jobId
        }).then(
            function onSuccess(response) {
                var jobInfo = response.data;

                result = {
                    success: true,
                    id: jobInfo.id,
                    jobstatus: jobInfo.jobStatus,
                    steprunning: getStepRunning(job),
                    stepsCompleted: getStepsCompleted(job)
                };
                deferred.resolve(result);
            }, function onError(resposne) {
                
            }
        );

        return deferred.promise;
        
    };
    
    function getStepRunning(job) {
        if (job.jobStatus != "Running") {
            return null;
        }
        
        for (var i = 0; i < job.steps.length; i++) {
            if (job.steps[i].jobStatus == "Running") {
                return job.steps[i].jobStepType;
            }
        }
        return null;
    };
    
    function getStepsCompleted(job) {
        if (job.jobStatus == "Pending") {
            return null;
        }
        
        var stepsCompleted = [];
        for (var i = 0; i < job.steps.length; i++) {
            if (jobs.steps[i].jobStatus == "Completed") {
                stepsCompleted.push(jobs.steps[i].jobStepType);
            }
        }
        return stepsCompleted;
    }
});
