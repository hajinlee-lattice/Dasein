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
.service('JobsStore', function($q, $filter, JobsService) {
    var JobsStore = this;
    this.importJobsMap = {};
    this.subjobsRunnigMap = {};
    this.data = {
        jobs: [],
        importJobs:[],
        subjobsRunning:[],
        loadingJobs: false,
        models: {},
        jobsMap: {},
        isModelState: false
    };

    function isImportJob(job){
        if(job.jobType === 'processAnalyzeWorkflow'){
            return true;
        }else{
            return false;
        }
    }

    function isImportSubJob(job){
        switch (job.jobType) {
            case 'cdlDataFeedImportWorkflow': 
            case 'cdlOperationWorkflow':
            case 'metadataChange':{
                return true;
            };
            default: {
                return false;
            }
        }
    }

    this.getJob = function(jobId) {
        var deferred = $q.defer();

        JobsService.getJobStatus(jobId).then(function(response) {
            if(isImportJob(response.resultObj)){
                JobsStore.addImportJob(response.resultObj);
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
                    var idNullImportJobs = false;

                    for (var i=0; i<res.length; i++) {
                        var job = res[i];

                        if (job.startTimestamp != null) {
                            JobsStore.addJobMap(job.id, job);
                            JobsStore.addJob(job, modelId);
                            if(isImportJob(job) && job.id == null){
                                idNullImportJobs = true;
                            }
                        }
                    }
                    JobsStore.synchImportJobs(idNullImportJobs, res);
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
            if(isImportJob(job)){
                JobsStore.addImportJob(job);
            }else if (isImportSubJob(job)){
                JobsStore.manageSubjobsRunning(job);
            }else{
                JobsStore.data.jobs.push(job);
            }
        }
    };

    this.addJobMap = function(jobId, job) {
        this.data.jobsMap[jobId] = job;
    };

    this.removeJob = function(jobId) {
        delete this.data.jobsMap[jobId];
    };

    this.runJob = function(job) {
        var deferred = $q.defer();

        JobsService.runJob(job).then(function(resp){
            deferred.resolve(job);
        });
        
        return deferred.promise;
        
    };
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

    this.manageSubjobsRunning = function(job){
        var applicationidJob = job.applicationId;
        var inMap = JobsStore.subjobsRunnigMap[applicationidJob];
        switch(job.jobStatus){
            case 'Completed' :
            case 'Failed' : {
                if(inMap !== undefined){
                    JobsStore.data.subjobsRunning.splice(inMap, 1);
                    delete JobsStore.subjobsRunnigMap[inMap];
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

    this.synchImportJobs = function(inNullId, retApi){
        if((inNullId == true &&  JobsStore.importJobsMap[null] == undefined) || (inNullId == false && JobsStore.importJobsMap[null]) != undefined){
            JobsStore.data.importJobs.splice(0, JobsStore.data.importJobs.length);
            JobsStore.importJobsMap = {};
            for(var i = 0; i < retApi.length; i++){
                if(isImportJob(retApi[i])){
                    JobsStore.data.importJobs.push(retApi[i]);
                    JobsStore.importJobsMap[retApi[i].id] = JobsStore.data.importJobs.length - 1;
                }
            }
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
