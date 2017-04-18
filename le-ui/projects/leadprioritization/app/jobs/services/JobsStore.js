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
        var defer = $q.defer();
        defer.resolve(this.data.dataImportJobs)
        return defer.promise;
    };

    this.data.dataImportJobs = [{"timestamp":1490971665695,"fileName":"Lattice_Full_location_20170331.csv","jobType":"Data Import","status":"Completed"},{"timestamp":1491075500934,"fileName":"Lattice_Full_location_20170401.csv","jobType":"Data Import","status":"Completed"},{"timestamp":1491257578363,"fileName":"Lattice_Full_location_20170403.csv","jobType":"Data Import","status":"Completed"},{"timestamp":1491512702355,"fileName":"Lattice_Full_location_20170406.csv","jobType":"Data Import","status":"Completed"},{"timestamp":1491667049385,"fileName":"Lattice_Full_location_20170408.csv","jobType":"Data Import","status":"Completed"},{"timestamp":1491938267633,"fileName":"Lattice_Full_location_20170411.csv","jobType":"Data Import","status":"Completed"},{"timestamp":1492016087146,"fileName":"Lattice_Full_location_20170412.csv","jobType":"Data Import","status":"Completed"},{"timestamp":1492369432966,"fileName":"Lattice_Full_location_20170416.csv","jobType":"Data Import","status":"Completed"},{"timestamp":1493132895501,"fileName":"Lattice_Full_location_20170425.csv","jobType":"Data Import","status":"Completed"},{"timestamp":1493403375791,"fileName":"Lattice_Full_location_20170428.csv","jobType":"Data Import","status":"Completed"},{"timestamp":1493649030170,"fileName":"Lattice_Full_location_20170501.csv","jobType":"Data Import","status":"Completed"},{"timestamp":1493840430170,"fileName":"Lattice_Full_location_20170503.csv","jobType":"Data Import","status":"Completed"}];
});
