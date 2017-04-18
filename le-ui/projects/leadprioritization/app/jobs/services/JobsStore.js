angular
.module('lp.jobs')
.run(function($timeout, $interval, $stateParams, $state, JobsStore) {
    $timeout(function() {
        JobsStore.getJobs();
    }, 1000); // FIXME: we wont need this soon, this is for switching tenants fix hack

    var pending = false;

    $interval(function() {
        var modelId = $stateParams.modelId || '';

        if (!pending) {
            pending = true;

            JobsStore.getJobs(null, modelId).then(function(response) {
                pending = false;
            });
        }
    }, 45 * 1000);
})
.service('JobsStore', function($q, JobsService) {
    var JobsStore = this;

    this.data = {
        jobs: [],
        models: {},
        jobsMap: {},
        dataImportJobs: [],
        isModelState: false
    };

    this.getJob = function(jobId) {
        var deferred = $q.defer();

        JobsService.getJobStatus(jobId).then(function(response) {
            deferred.resolve(response.resultObj);
        });

        return deferred.promise;
    };

    this.getJobs = function(use_cache, modelId) {
        var deferred = $q.defer(),
            isModelState = modelId ? true : false,
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
                    if (!JobsStore.data.models[modelId]) {
                        JobsStore.data.models[modelId] = [];
                    }

                    JobsStore.data.models[modelId].length = 0;

                    for (var i=0; i<response.length; i++) {
                        var job = response[i];

                        JobsStore.addJobMap(job.id, job);
                        JobsStore.addJob(job, modelId);
                    }
                } else {
                    JobsStore.data.jobs.length = 0;

                    for (var i=0; i<response.length; i++) {
                        var job = response[i];

                        if (job.startTimestamp != null) {
                            JobsStore.addJobMap(job.id, job);
                            JobsStore.addJob(job, modelId);
                        }
                    }
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

    this.getDataImportJobs = function() {
        var self = this;
        var defer = $q.defer();

        if (self.data.dataImportJobs.length > 0) {
            defer.resolve(self.data.dataImportJobs)
        } else {
            JobsService.getDataImportJobs().then(function(response) {
                var data = Array.isArray(response.data) ? response.data : [];
                for (var i = 0; i < data.length; i++) {
                    self.data.dataImportJobs.push(data[i]);
                }
                defer.resolve(self.data.dataImportJobs);
            });
        }

        return defer.promise;
    };
});
