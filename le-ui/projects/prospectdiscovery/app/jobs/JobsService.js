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
                            jobStatus: jobInfo.jobStatus,
                            stepRunning: getStepRunning(jobInfo),
                            stepsCompleted: getStepsCompleted(jobInfo)
                        }
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
            if (job.steps[i].stepStatus == "Running") {
                return job.steps[i].jobStepType;
            }
        }
        return null;
    };
    
    function getStepsCompleted(job) {
        if (job.steps == null) {
            return [];
        }

        var stepsCompleted = [];
        for (var i = 0; i < job.steps.length; i++) {
            if (job.steps[i].stepStatus == "Completed") {
                stepsCompleted.push(job.steps[i].jobStepType);
            }
        }
        return stepsCompleted;
    }
});
