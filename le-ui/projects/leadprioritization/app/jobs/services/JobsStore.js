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
    }, 120 * 1000);
})
.service('JobsStore', function($q, $filter, JobsService) {
    var JobsStore = this;
    this.importJobsMap = {};
    this.exportJobsMap = {};
    this.subjobsRunnigMap = {};
    this.jobTypes = ['import', 'export'],
    this.data = {
        jobs: [],
        importJobs:[],
        exportJobs: [],
        subjobsRunning:[],
        loadingJobs: false,
        models: {},
        jobsMap: {},
        isModelState: false
    };
    this.inProgressModelJobs = {};
    this.cancelledJobs = {};

    function isImportJob(job){
        if(job.jobType === 'processAnalyzeWorkflow'){
            return true;
        }else{
            return false;
        }
    }

    function isExportJob(job) {
        return (job.jobType === 'segmentExportWorkflow');
    }

    function isType(job, type) {
        switch (type) {
            case 'import':
                return isImportJob(job);
            case 'export':
                return isExportJob(job);
        }
    }

    this.isJobsEverFetched = function(){
        if(this.data.jobs.length === 0 && this.data.importJobs.length === 0 && this.data.exportJobs.length === 0){
            return false;
        }else{
            return true;
        }
    }

    this.isNonWorkflowJobType = function(job) {
        return job.id == null && job.pid == null;
    }

    this.getJob = function(jobId) {
        var deferred = $q.defer();

        JobsService.getJobStatus(jobId).then(function(response) {
            if (isImportJob(response.resultObj)){
                JobsStore.addImportJob(response.resultObj);
            } else if (isExportJob(response.resultObj)) {
                JobsStore.addExportJob(response.resultObj);
            }
            deferred.resolve(response.resultObj);
        });

        return deferred.promise;
    };

    this.getJobFromApplicationId = function(jobApplicationId) {
        var deferred = $q.defer();

        JobsService.getJobStatusFromApplicationId(jobApplicationId).then(function(response) {
            if (response.success) {
                var resultObj = response.resultObj;
                deferred.resolve(resultObj);
            }
        });

        return deferred.promise;
    }


    this.getJobs = function(use_cache, modelId) {
        JobsStore.loadingJobs = true;
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
                var res = response.resultObj;
                if (modelId) {
                    if (!JobsStore.data.models[modelId]) {
                        JobsStore.data.models[modelId] = [];
                    }

                    JobsStore.data.models[modelId].length = 0;

                    for (var i=0; i<res.length; i++) {
                        var job = res[i];

                        JobsStore.addJobMap(job.id, job);
                        JobsStore.addJob(job, modelId);
                    }
                } else {
                    JobsStore.data.jobs.length = 0;

                    var nullIdsMap = {};
                    JobsStore.inProgressModelJobs = {};
                    JobsStore.jobTypes.forEach(function(type) {
                        nullIdsMap[type] = false;
                    })

                    for (var i=0; i<res.length; i++) {
                        var job = res[i];

                        if (job.startTimestamp != null) {
                            JobsStore.addJobMap(job.id, job);
                            JobsStore.addJob(job, modelId);
                            if(isImportJob(job) && job.id == null){
                                nullIdsMap['import'] = true;
                            } else if (isExportJob(job) && job.id == null) {
                                nullIdsMap['export'] = true;
                            }
                        }
                    }
                    JobsStore.synchJobs(nullIdsMap, res);
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
            switch (job.jobType) {
                case 'processAnalyzeWorkflow':
                    JobsStore.addImportJob(job);
                    break;
                case 'segmentExportWorkflow':
                    JobsStore.addExportJob(job);
                    break;
                case 'cdlDataFeedImportWorkflow': 
                case 'cdlOperationWorkflow':
                case 'metadataChange': {
                    JobsStore.manageSubjobsRunning(job);
                    break;
                };
                default:
                    JobsStore.addModelJob(job);
                    break;
            }
        }
    };

    this.getMap = function(type) {
        switch (type) {
            case 'import':
                return JobsStore.importJobsMap;
            case 'export':
                return JobsStore.exportJobsMap;
            default:
                return {};
        }
    }

    this.getList = function(type) {
        switch (type){
            case 'import':
                return JobsStore.data.importJobs;
            case 'export':
                return JobsStore.data.exportJobs;
            default:
                return [];
        }
    }

    this.addJobMap = function(jobId, job) {
        this.data.jobsMap[jobId] = job;
    };

    this.removeJob = function(jobId) {
        delete this.data.jobsMap[jobId];
    };

    this.runJob = function(job) {
        var deferred = $q.defer();

        JobsService.runJob(job).then(function(resp){
            console.log('Run job resp ',resp);
            deferred.resolve(resp);
        });
        
        return deferred.promise;
        
    };

    this.addModelJob = function(job) {
        var ratingEngineId = job.inputs.RATING_ENGINE_ID;
        if (job.jobStatus != 'Failed' && job.jobStatus != 'Completed' && job.jobStatus != 'Cancelled' && JobsStore.cancelledJobs[ratingEngineId] == undefined) {
            //console.log('in progress job.id', job.id);
            JobsStore.inProgressModelJobs[ratingEngineId] = job.id;
        } else if (job.jobStatus == 'Cancelled' && JobsStore.cancelledJobs[ratingEngineId] != undefined) {
            delete JobsStore.cancelledJobs[ratingEngineId];
        }
        JobsStore.data.jobs.push(job);
    }

    this.addImportJob = function(job){
        job.displayName = "Data Processing & Analysis";
        var jobid = job.id;
        var inMap = JobsStore.importJobsMap[jobid];
        if(inMap === undefined){
            JobsStore.data.importJobs.push(job);
            JobsStore.importJobsMap[jobid] = JobsStore.data.importJobs.length - 1;
        }else {
            JobsStore.data.importJobs[inMap].jobStatus = job.jobStatus;
            updateSubJobsImportJob(job);
            updateStepsCompleted(job);
            
        }
    }

    this.addExportJob = function(job) {
        job.displayName = "Segment Export";
        var jobId = job.id;
        var inMap = JobsStore.exportJobsMap[jobId];
        if (inMap === undefined) {
            JobsStore.data.exportJobs.push(job);
            JobsStore.exportJobsMap[jobId] = JobsStore.data.exportJobs.length - 1;
        } else {
            JobsStore.data.exportJobs[inMap].jobStatus = job.jobStatus;
        }
    }


    this.getDisplayName = function(type) {
        switch (type) {
            case 'import':
                return 'Data Processing & Analysis';
            case 'export':
                return 'Segment Export';
            default:
                console.log('job type not defined');
                return '';
        }
    }

    this.manageSubjobsRunning = function(job){
        var applicationidJob = job.applicationId;
        var inMap = JobsStore.subjobsRunnigMap[applicationidJob];
        switch(job.jobStatus){
            case 'Completed' :
            case 'Failed' : {
                if(inMap !== undefined){
                    JobsStore.data.subjobsRunning.splice(inMap, 1);
                    JobsStore.subjobsRunnigMap = {};
                    for(var i=0; i<JobsStore.data.subjobsRunning.length; i++){
                        var appId = JobsStore.data.subjobsRunning[i].applicationId;
                        JobsStore.subjobsRunnigMap[appId] = i;
                    }
                }
                break;
            }
            
            default: {
                if(inMap === undefined){
                    JobsStore.data.subjobsRunning.push(job);
                    JobsStore.subjobsRunnigMap[applicationidJob] = JobsStore.data.subjobsRunning.length - 1;
                }
                break;
            }
        }
    }

    this.synchJobs = function(nullIdsMap, retApi) {
        this.jobTypes.forEach(function(type) {
            var jobsMap = JobsStore.getMap(type);
            var jobsList = JobsStore.getList(type);
            if ((nullIdsMap[type] == true && jobsMap[null] == undefined) || (nullIdsMap[type] == false && jobsMap[null]) != undefined) {
                jobsList.splice(0, jobsList.length);
                jobsMap = {};
                for (var i = 0; i < retApi.length; i++) {
                    if (isType(retApi[i], type)) {
                        jobsList.push(retApi[i]);
                        jobsMap[retApi[i].id] = jobsList.length - 1;
                    }
                }
                JobsStore.setJobsByType(jobsMap, jobsList, type);
            }
        });
    }

    this.setJobsByType = function(map, list, type) {
        if (type == 'import') {
            JobsStore.importJobsMap = map;
            JobsStore.data.importJobs = list;
        } else if (type == 'export') {
            JobsStore.exportJobsMap = map;
            JobsStore.data.exportJobs = list;
        }
    }


    this.clearImportJobs = function(){
        JobsStore.data.importJobs = [];
        JobsStore.importJobsMap = {};   
    }

    /**
     * Updates the fields in the job
     * The arrays should not be updated otherwise the view that relys on them will refresh and loose its state
     * @param {*} updatedJob 
     */
    function updateSubJobsImportJob(updatedJob){
        // console.log(updatedJob);
        var jobid = updatedJob.id;
        var inMap = JobsStore.importJobsMap[jobid];
        if(inMap !== undefined){
            var oldJob = JobsStore.data.importJobs[inMap];
            if(oldJob !== undefined){
                oldJob.subJobs.splice(0, oldJob.subJobs.length);
                for (var i = 0; i < updatedJob.subJobs.length; i++) {
                    oldJob.subJobs.push(updatedJob.subJobs[i]);
                }
            }
        }
    }

    function updateStepsCompleted(updatedJob){
        var jobid = updatedJob.id;
        var inMap = JobsStore.importJobsMap[jobid];
        if(inMap !== undefined){
            var oldJob = JobsStore.data.importJobs[inMap];
            if(oldJob !== undefined){
                oldJob.steps.splice(0, oldJob.steps.length);
                for (var i = 0; i < updatedJob.steps.length; i++) {
                    oldJob.steps.push(updatedJob.steps[i]);
                }
            }
        }
    }
});
