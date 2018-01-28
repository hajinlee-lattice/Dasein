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
    this.tmpCount = -1;
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

    this.getJob = function(jobId) {
        var deferred = $q.defer();

        JobsService.getJobStatus(jobId).then(function(response) {
            updateJobTesting(response.resultObj);
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
                    // JobsStore.data.importJobs = [];
                    for (var i=0; i<res.length; i++) {
                        var job = res[i];

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
            if(job.jobType === "processAnalyzeWorkflow"){
                job.displayName = "Data Processing & Analysis";
                var jobid = job.id;
                var inMap = JobsStore.importJobsMap[jobid];
                /************* Only for testing ******************/
                // updateStateTest(job); 
                /*************************************************/
                if(inMap === undefined){
                    JobsStore.data.importJobs.push(job);
                    JobsStore.importJobsMap[jobid] = JobsStore.data.importJobs.length - 1;
                }else {
                    updateFieldsImportJob(job);
                }
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
        var jobid = updatedJob.id;
        var inMap = JobsStore.importJobsMap[jobid];
        if(inMap !== undefined){
            var oldJob = JobsStore.data.importJobs[inMap];
            oldJob.jobStatus = updatedJob.jobStatus;

            console.log(oldJob);
        }
    }

    // function updateStepTESTING(status, jobid){

    //     var inMap = JobsStore.importJobsMap[jobid];
    //     if(inMap !== undefined){

    //         var oldJob = JobsStore.data.importJobs[inMap];
    //         oldJob.jobStatus = updatedJob.jobStatus;
    //         console.log(oldJob);
            
    //     }

    // }

    function updateStateTest(job) {
        switch(JobsStore.tmpCount){
            case 0:
            case 1:
            case 7:{
                job.jobStatus = 'Pending';
                break;
            };
            case 2:
            case 3:
            case 4:
            case 5:
            case 8:
            case 9:{
                job.jobStatus = 'Running';
                break;
            };
            case 6:{
                job.jobStatus = 'Completed';
               
                break;
            };
            case 10:{
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
    /**
     * Import Jobs testing method
     * @param {*} job 
     */
    function updateJobTesting(job){
        /******************* For testing porpouse ***********/
        console.log('Iteration' ,JobsStore.tmpCount);
        switch(JobsStore.tmpCount){
            // case 1:{
            //     job.jobStatus = 'Pending';
            //     break;
            // };
            // case 2:{
            //     job.jobStatus = 'Running';
            //     break;
            // };
            case 3:{
                // job.jobStatus = 'Running';
                job.stepsCompleted = ["Merging, De-duping & matching to Lattice Data Cloud", 
                                        "Merging, De-duping & matching to Lattice Data Cloud"];
                break;
            };
            case 4:{
                // job.jobStatus = 'Running';
                job.stepsCompleted = ["Merging, De-duping & matching to Lattice Data Cloud", 
                                        "Merging, De-duping & matching to Lattice Data Cloud",
                                    "Analyzing", "Analyzing", "Analyzing"];
                break;
            };
            case 5:{
                // job.jobStatus = 'Running';
                job.stepsCompleted = ["Merging, De-duping & matching to Lattice Data Cloud", 
                                        "Merging, De-duping & matching to Lattice Data Cloud",
                                    "Analyzing", "Analyzing", "Analyzing",
                                "Publishing", "Publishing", "Publishing"];
                
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
                job.stepsCompleted = [];
                break;
            };
            case 9:{
                job.stepsCompleted = ["Merging, De-duping & matching to Lattice Data Cloud", 
                "Merging, De-duping & matching to Lattice Data Cloud"];
                break;
            };
            case 10:{
                job.stepsCompleted = ["Merging, De-duping & matching to Lattice Data Cloud", 
                "Merging, De-duping & matching to Lattice Data Cloud"];
                break;
            };
           
            case 11:{
                JobsStore.tmpCount = 0;
                job.stepsCompleted = [];
                // job.jobStatus = 'Pending';
                break;
            };
            default: {
                break;
            }
        }

        /**************** End test porpose ******************************/
    }

    // this.data.dataImportJobs = [{"timestamp":1490971665695,"fileName":"Lattice_Full_location_20170331.csv","jobType":"Data Import","status":"Completed"},
    //                             {"timestamp":1491075500934,"fileName":"Lattice_Full_location_20170401.csv","jobType":"Data Import","status":"Pending"},
    //                             {"timestamp":1491257578363,"fileName":"Lattice_Full_location_20170403.csv","jobType":"Data Import","status":"Running"},
    //                             {"timestamp":1491512702355,"fileName":"Lattice_Full_location_20170406.csv","jobType":"Data Import","status":"Failed"},
    //                             {"timestamp":1491667049385,"fileName":"Lattice_Full_location_20170408.csv","jobType":"Data Import","status":"Cancelled"},
    //                             {"timestamp":1491938267633,"fileName":"Lattice_Full_location_20170411.csv","jobType":"Data Import","status":"Completed"},
    //                             {"timestamp":1492016087146,"fileName":"Lattice_Full_location_20170412.csv","jobType":"Data Import","status":"Pending"},
    //                             {"timestamp":1492369432966,"fileName":"Lattice_Full_location_20170416.csv","jobType":"Data Import","status":"Running"},
    //                             {"timestamp":1493132895501,"fileName":"Lattice_Full_location_20170425.csv","jobType":"Data Import","status":"Failed"},
    //                             {"timestamp":1493403375791,"fileName":"Lattice_Full_location_20170428.csv","jobType":"Data Import","status":"Cancelled"},
    //                             {"timestamp":1493649030170,"fileName":"Lattice_Full_location_20170501.csv","jobType":"Data Import","status":"Completed"},
    //                             {"timestamp":1493840430170,"fileName":"Lattice_Full_location_20170503.csv","jobType":"Data Import","status":"Pending"}];
});
