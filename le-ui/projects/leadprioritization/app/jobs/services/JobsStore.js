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
    /******************* For testing *********************************/
    // this.tmpCount = -1;
    /*****************************************************************/
    this.importJobsMap = {};
    this.data = {
        jobs: [],
        importJobs:[],
        loadingJobs: false,
        models: {},
        jobsMap: {},
        isModelState: false,
        dataProcessingRunningJob: {}

    };

    function isImportJob(job){
        if(job.jobType === 'processAnalyzeWorkflow'){
            return true;
        }else{
            return false;
        }
    }

    this.getJob = function(jobId) {
        var deferred = $q.defer();

        JobsService.getJobStatus(jobId).then(function(response) {
            /**************** Only for Testing *****************************/
            // if(jobId == 0){
            //     updateJobTesting(response.resultObj);
            // }
            /***************************************************************/

            deferred.resolve(response.resultObj);
        });

        return deferred.promise;
    };


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
                /************* Only for testing ****************************/
                // JobsStore.tmpCount = JobsStore.tmpCount + 1;
                /***********************************************************/
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
                /******************** For Testing *************************/
                // if(JobsStore.tmpCount >= 2 && JobsStore.tmpCount < 4){
                //     job.id = null;
                // }
                /**********************************************************/

                JobsStore.addImportJob(job);
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
        dataProcessingRunningJob = job;
        var deferred = $q.defer();

        JobsService.runJob(job).then(function(resp){
            
            if(resp.Success && resp.Success === true){
                job.jobStatus = 'Running';
            }else{
                job.jobStatus = 'Failed';
                vm.dataProcessingRunningJob = {};
            }
            deferred.resolve(job);
        });
        
        return deferred.promise;
        
    };
    this.addImportJob = function(job){
        job.displayName = "Data Processing & Analysis";
        var jobid = job.id;
        // console.log('Add job id',job);
        var inMap = JobsStore.importJobsMap[jobid];
        // console.log('Is in Map',inMap);
        /************* Only for testing ******************/
        // if(job.id === 0 || job.id == null) {
        //     updateStateTest(job); 
        // }
        /*************************************************/
        if(inMap === undefined){
            JobsStore.data.importJobs.push(job);
            JobsStore.importJobsMap[jobid] = JobsStore.data.importJobs.length - 1;
        }else {
            // JobsStore.data.importJobs[inMap] = "qux";
            // console.log('UPDTAING');
            updateFieldsImportJob(job);
            
        }
    }

    this.synchImportJobs = function(inNullId, retApi){
        
        if((inNullId == true &&  JobsStore.importJobsMap[null] == undefined) || (inNullId == false && JobsStore.importJobsMap[null]) != undefined){
            // console.log(retApi);
            JobsStore.data.importJobs.splice(0, JobsStore.data.importJobs.length);
            JobsStore.importJobsMap = {};
            
            for(var i = 0; i < retApi.length; i++){
                if(isImportJob(retApi[i])){
                    JobsStore.data.importJobs.push(retApi[i]);
                    JobsStore.importJobsMap[retApi[i].id] = JobsStore.data.importJobs.length - 1;
                }
            }
            // console.log('++++++++++++ After update ++++++++++++');
            // console.log(JobsStore.data.importJobs);
            // console.log(JobsStore.importJobsMap);
            // console.log('++++++++++++++++++++++++++++');
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
    function updateFieldsImportJob(updatedJob){
        // console.log(updatedJob);
        var jobid = updatedJob.id;
        var inMap = JobsStore.importJobsMap[jobid];
        if(inMap !== undefined){
            var oldJob = JobsStore.data.importJobs[inMap];
            if(oldJob !== undefined){
                oldJob.jobStatus = updatedJob.jobStatus;
                oldJob.inputs = updatedJob.inputs;
            }
        }
        // console.log('************************************');
    }

    /**************************************** Methods for testing ****************************************/

    function updateStateTest(job) {
        console.log('Iteration' ,JobsStore.tmpCount);
        switch(JobsStore.tmpCount){
            case 0:
            case 7:{
                job.jobStatus = 'Pending';
                break;
            };
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 5:
            case 8:
            case 9:
           {
                job.jobStatus = 'Running';
                break;
            };
            case 6:{
                job.jobStatus = 'Completed';
               
                break;
            };
            case 10:
            {
                job.jobStatus = 'Failed';
               
                break;
            };
            case 11:{
                JobsStore.tmpCount = 0;
                job.jobStatus = 'Pending';
                break;
            };
        }
            
    }
    
    function updateJobTesting(job){
        // console.log('Iteration' ,JobsStore.tmpCount);
        switch(JobsStore.tmpCount){
            case 1:{
                job.stepsCompleted = [];
                break;
            };
            case 2:{
                job.stepsCompleted = ["Merging, De-duping & matching to Lattice Data Cloud"];
                break;
            };
            case 3:{
                job.stepsCompleted = ["Merging, De-duping & matching to Lattice Data Cloud", 
                                        "Analyzing"];
                break;
            };
            case 4:{
                job.stepsCompleted = ["Merging, De-duping & matching to Lattice Data Cloud", 
                                        "Merging, De-duping & matching to Lattice Data Cloud",
                                    "Analyzing", "Analyzing", "Publishing"];
                break;
            };
            case 5:{
                job.stepsCompleted = ["Merging, De-duping & matching to Lattice Data Cloud", 
                                        "Merging, De-duping & matching to Lattice Data Cloud",
                                    "Analyzing", "Analyzing", "Analyzing",
                                "Publishing", "Publishing", "Publishing", "Scoring"];
                
                break;
            };
            case 6:{
                // job.jobStatus = 'Running';
                job.stepsCompleted = ["Merging, De-duping & matching to Lattice Data Cloud", 
                "Merging, De-duping & matching to Lattice Data Cloud",
                "Analyzing", "Analyzing", "Analyzing",
                "Publishing", "Publishing", "Publishing",
                "Scoring", "Scoring"];
                break;
            };
            case 7:{
                // job.jobStatus = 'Completed';
                job.stepsCompleted = [];
                break;
            };
            case 8:{
                // job.jobStatus = 'R';
                job.stepsCompleted = ["Merging, De-duping & matching to Lattice Data Cloud"];
                break;
            };
            case 9:{
                job.stepsCompleted = ["Merging, De-duping & matching to Lattice Data Cloud", 
                "Analyzing"];
                break;
            };
            case 10:{
                job.stepsCompleted = ["Merging, De-duping & matching to Lattice Data Cloud", 
                "Merging, De-duping & matching to Lattice Data Cloud", "Analyzing"];
                break;
            };
           
            case 11:{
                job.stepsCompleted = [];
                break;
            };
            default: {
                break;
            }
        }
        /**************** End test porpose ******************************/
    }
});
