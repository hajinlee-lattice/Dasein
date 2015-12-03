angular.module('services.jobs', [
])

.service('JobsService', function($http, $q, _) {
    this.GetAllJobs = function() {
        var deferred = $q.defer();
        var result;
/*
        $http({
            method: 'GET',
            url: '/pls/jobs'
        }).then(
            function onSuccess(response) { */
                //var jobs = response.data;
                var jobs = [{"id":1,"name":null,"description":null,"startTimestamp":1449106526700,"endTimestamp":null,"jobStatus":"Running","jobType":"Loadfile_import","user":"LatticeUser1","steps":null}];
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
                deferred.resolve(result);/*
            }, function onError(response) {

            }
        );
*/
        return deferred.promise;
    };
    
    this.getJobStatus = function(jobId) {
        var deferred = $q.defer();
        var result;
/*
        $http({
            method: 'GET',
            url: '/pls/jobs/' + jobId
        }).then(
            function onSuccess(response) { */
                //var jobInfo = response.data;
                var jobInfo = {"id":1,"name":null,"description":null,"startTimestamp":1448930225355,"endTimestamp":null,"jobStatus":"Running","jobType":"Loadfile_import","user":"LatticeUser1","steps":[{"name":null,"description":null,"startTimestamp":1448930225355,"endTimestamp":1448930225355,"jobStatus":"Completed","jobStepType":"Load_data"},{"name":null,"description":null,"startTimestamp":1448930225355,"endTimestamp":1448930225355,"jobStatus":"Completed","jobStepType":"Match_data"},{"name":null,"description":null,"startTimestamp":1448930225355,"endTimestamp":1448930225355,"jobStatus":"Completed","jobStepType":"Generate_insights"},{"name":null,"description":null,"startTimestamp":1448930225355,"endTimestamp":1448930225355,"jobStatus":"Completed","jobStepType":"Create_model"},{"name":null,"description":null,"startTimestamp":1448930225355,"endTimestamp":null,"jobStatus":"Running","jobStepType":"Create_global_target_market"}]};

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
                deferred.resolve(result);/*
            }, function onError(resposne) {
                
            }
        );
*/
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
