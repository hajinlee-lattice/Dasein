angular
.module('lp.jobs')
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
});