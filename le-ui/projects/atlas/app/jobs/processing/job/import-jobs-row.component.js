angular.module('lp.jobs.import.row', [
    'common.modal', 'mainApp.appCommon.services.HealthService'
])
.directive('importJobRow', [function () {
    var controller = ['$scope', '$q', '$filter', 'JobsStore', 'Modal', 'AuthorizationUtility', 'HealthService', function ($scope, $q, $filter, JobsStore, Modal, AuthorizationUtility, HealthService) {
        $scope.thejob = $scope.job;
        $scope.disableButton = false;
        $scope.scheduleButtonDisabled = false;
        $scope.maxRowsTooltip = 3;
        $scope.expanded = false;
        $scope.chevronConfig = {
            0: { name: 'Merging, De-duping & matching to Lattice Data Cloud', lable: 'Merging, De-duping & Matching' },
            1: { name: 'Analyzing', lable: 'Analyzing' },
            2: { name: 'Publishing', lable: 'Loading' },
            3: { name: 'Scoring', lable: 'Scoring' }
        };

        $scope.stepsConfig = {
            "Merging, De-duping & matching to Lattice Data Cloud": { position: 1, label: 'Merging, De-duping & Matching' },
            'Analyzing': { position: 2, label: 'Analyzing' },
            'Publishing': { position: 3, label: 'Loading' },
            'Scoring': { position: 4, label: 'Scoring' }
        };


        function callbackModalWindow(action) {
            var modal = Modal.get('processJob_Warning');
            
            
            if (action && action.action === 'ok') {
                if(modal){
                    modal.waiting(true);
                }
               
                $scope.job.jobStatus = 'Waiting';
                $scope.disableButton = true;
                JobsStore.runJob($scope.job).then(function (result) {
                    if(modal){
                        Modal.modalRemoveFromDOM(modal, {name: 'processJob_Warning'});
                    }
                    $scope.disableButton = true;
                    if(result.Success === true) {
                        $scope.job.jobStatus = 'Pending';
                    } 
                });
            }else if("closedForced" == action.action){
                $scope.disableButton = false;
                setTimeout(() => {
                    $scope.$apply(()=>{});
                },0);
            }else {
                $scope.disableButton = false;
                if(modal){
                    Modal.modalRemoveFromDOM(modal, {name: 'processJob_Warning'});
                }
                setTimeout(() => {
                    $scope.$apply(()=>{
                        
                    });
                },0);
            }
        }
        
        function init() {
            // console.log('THE JOB ', $scope.job.startTimestamp, $scope.job.endTimestamp);


            if ($scope.vm.rowStatus[$scope.index] != undefined && $scope.vm.rowStatus[$scope.index] == true) {
                $scope.expanded = true;
            }
            $scope.loading = false;
        }

        function getRecordFound(subjob) {
            if (subjob.reports && subjob.reports.length > 0) {
                var json = subjob.reports[0].json.Payload;
                var obj = JSON.parse(json);
                return obj.total_rows;
            } else {
                return '-';
            }
        }

        function getRecordUploaded(subjob) {
            if (subjob.reports && subjob.reports.length > 0) {
                var json = subjob.reports[0].json.Payload;
                var obj = JSON.parse(json);
                return obj.imported_rows;
            } else {
                return '-';
            }
        }
        function getPayloadValue(subjob, field) {
            if (subjob.reports && subjob.reports.length > 0) {
                var json =
                    subjob.reports[0].json
                        .Payload;
                var obj = JSON.parse(json);
                var ret =
                    obj[field] != undefined
                        ? obj[field]
                        : "-";
                return ret;
            } else {
                return "-";
            }
        }

        $scope.getSubJobsPartialSuccess = function () {
            var listPartialSuccess = [];
            for (var i = 0; i < $scope.job.subJobs.length; i++) {
                var found = getRecordFound($scope.job.subJobs[i]);
                var uploaded = getRecordUploaded($scope.job.subJobs[i]);
                if (found != uploaded && $scope.job.subJobs[i].inputs != undefined && $scope.job.subJobs[i].jobStatus != "Cancelled" ) {
                    listPartialSuccess.push($scope.job.subJobs[i]);
                }
            }
            return listPartialSuccess;
        };

        $scope.getSubjobActionName = function (index, subjob) {
            if (subjob.inputs != undefined) {
                return index + '. ' + subjob.inputs.SOURCE_DISPLAY_NAME;
            } else {
                return index + '. Unknown';
            }
        };

        $scope.isOneActionCompleted = function (job) {
            var subJobs = job.subJobs;
            var oneCompleted = false;
            if (subJobs && subJobs.length > 0) {
                subJobs.forEach(function (job) {
                    if (job.jobStatus !== 'Running' || job.jobStatus !== 'Pending') {
                        oneCompleted = true;
                        return oneCompleted;
                    }
                });
            }else{
                oneCompleted = true;
            }
            if(job.jobStatus === 'Ready' && oneCompleted === false){
                job.jobStatus = 'Waiting';
            }
            return oneCompleted;
        };

        $scope.expandRow = function () {
            $scope.loading = false;
            $scope.expanded = !$scope.expanded || false;
            $scope.vm.rowStatus[$scope.index] = $scope.expanded;
        };

        $scope.run = function (job) {
            var show = $scope.showWarningRun(job);
            var msg = $scope.getWarningMessage(job);
            if (msg != null) {
                Modal.warning({
                    name: 'processJob_Warning',
                    title: "Run Job",
                    message: msg,
                    confirmtext: "Yes, Run"
                }, callbackModalWindow);
            } else {
                callbackModalWindow({action: 'ok'});
            }
        };
        
        $scope.schedule = (job) => { 
            let showWarning = $scope.showWarningSchedule(job);
            if (showWarning) { 
                Modal.warning(
					{
						name: "processJob_Warning",
						title: "Schedule Job",
						message:
							"<p>The data refresh in your previous job failed to succeed. </p><p>Re-import your previous data if you need them in your latest data refresh</p>",
						confrmtext: "Yes, Schedule"
					},
					callbackModalWindow
				);
            } else {
                if (job.schedulingInfo) {
					job.schedulingInfo.scheduled = true;
				} else {
					job.schedulingInfo = {};
                    job.schedulingInfo.scheduled = true;
                    job.schedulingInfo.schedulerEnabled = true;
                    
				}
                callbackModalWindow({ action: "ok" });
            }
            
            
        }

        $scope.getWarningMessage = function(job){
            var formerFailed = $scope.vm.isLastOneFailed();
            var someIncompleted = $scope.showWarningRun(job);
            var msg = null;
            if(formerFailed === true){
                if(someIncompleted){
                    msg = "<p>The data refresh in your previous job failed to succeed. </p><p>Re-import your previous data if you need them in your latest data refresh</p><br><br><p>Some actions are not completed yet. If you wish to run the Data Processing Job now, only the completed actions will be taken. </p><p>The actions are still running the validation will be queued to the next Processing Job.</p><p>You won't be able to run the next job until the current job is done.</p>";
                }else{
                    msg = "<p>The data refresh in your previous job failed to succeed. </p><p>Re-import your previous data if you need them in your latest data refresh</p>";
                }
            }else if (someIncompleted){
                msg = "<p>Some actions are not completed yet. If you wish to run the Data Processing Job now, only the completed actions will be taken. </p><p>The actions are still running the validation will be queued to the next Processing Job.</p><p>You won't be able to run the next job until the current job is done.</p>";
            }
            return msg;
        }

        $scope.showWarningSchedule = (job) => { 
            let oneFailed = false;
            let subjobs = (job.subJobs && job.subJobs != null) ? job.subJobs : [];
            for (let i = 0; i < subjobs.length; i++) { 
                if (subjobs[i].jobStatus == 'Failed' || $scope.isOneJobFailed(subjobs)) {
                    oneFailed = true;
                    break;
                }
            }
            return oneFailed;
        }
        $scope.isOneJobFailed = (subjobs) => { 
            let oneFailed = false;
            for (let i = 0; i < subjobs.length; i++){
                if (
					(!isNaN(
						getPayloadValue(
							subjobs[i],
							"total_failed_rows"
						)
					) &&
						!isNaN(
							getPayloadValue(
								subjobs[i],
								"total_rows"
							)
						) &&
						getPayloadValue(
							subjobs[i],
							"total_failed_rows"
						) ===
							getPayloadValue(
								subjobs[i],
								"total_rows"
							)) ||
					getPayloadValue(
						subjobs[i],
						"total_failed_rows"
					) ==
						getPayloadValue(
							subjobs[i],
							"total_rows"
						)
				) {
                    oneFailed = true;
                    break;
				}
            }
            return oneFailed;

            // if (!isNaN(getPayloadValue(subjob, 'total_failed_rows')) && !isNaN(getPayloadValue(subjob, 'total_rows')) && getPayloadValue(subjob, 'total_failed_rows') === getPayloadValue(subjob, 'total_rows') || (getPayloadValue(subjob, 'total_failed_rows') == getPayloadValue(subjob, 'total_rows'))) {
            //         return 'Failed';
            //     }
        }
        

        $scope.showWarningRun = function (job) {
            var subJobs = job.subJobs;

            var allCompleted = true;
            if (subJobs) {
                for (var i = 0; i < subJobs.length; i++) {
                    if (subJobs[i].jobStatus != 'Completed') {
                        allCompleted = false;
                        break;
                    }
                }
            }
            return !allCompleted;
        };
        
        $scope.getTimeStamp = function (job) {
            if((job.endTimestamp !== null && job.jobStatus != 'Ready')){
                let ret = $filter('date')(job.startTimestamp, 'MM/dd/yyyy h:mma');
                return ret;
            }else{
                return '';
            }
        };
        
        $scope.mouseDownRun  = function(job){
            HealthService.checkSystemStatus().then(function() {
                $scope.disableButton = true;
                $scope.run(job);
            });

           
        };
        
        $scope.getJobStatus = function(job){
            return job.jobStatus;
            
        };
        $scope.getJobStatusFn = function(job){
            return $scope.getJobStatus(job);
        }
        $scope.disableRunButton = function (job) {
            var oneCompleted = $scope.isOneActionCompleted(job);
            var canRun = $scope.vm.canLastJobRun();
            var disable = false;
            if ($scope.disableButton || !canRun || !oneCompleted) {
                disable = true;
            }
            return disable;
        };
        
        $scope.showRunButton = function (job) {
            if (job.schedulingInfo && job.schedulingInfo.schedulerEnabled == true) {
                return false;
            } else {
                switch (job.jobStatus) {
                    case "Failed":
                    case "Completed":
                    case "Pending":
                    case "Running": {
                        return false;
                    }
                    default: {
                        return true;
                    }
                }
            }
        };
                    
        $scope.mouseDownSchedule = (job) => { 
             HealthService.checkSystemStatus().then(function() {
					$scope.scheduleButtonDisabled = true;
					$scope.schedule(job);
				});

        }

        $scope.disableScheduleButton = (job) => { 
            // console.log(job.schedulingInfo);
            if (job.schedulingInfo && job.schedulingInfo.scheduled == true) {
                return true;
           }
            var canSchedule = $scope.vm.canLastBeScheduled();
            // console.log($scope.scheduleButtonDisabled);
            var disable = false;
            if ($scope.scheduleButtonDisabled || !canSchedule) {
				disable = true;
			}
            // console.log(disable);
            return disable;
        }
        $scope.showScheduleButton = function (job) {
            // console.log(job);
            if (
                !job.schedulingInfo ||
                job.schedulingInfo.schedulerEnabled !== true
            ) {
                return false;
            } else {
                switch (job.jobStatus) {
                    case "Failed":
                    case "Completed":
                    case "Pending":
                    case "Running":
                    case "Waiting": {
                        return false;
                    }
                    default: {
                        return true;
                    }
                }
            }
        }

        $scope.showReport = function (job) {
            if($scope.showRunButton(job)){
                return false;
            }else{
                switch(job.jobStatus){
                    case 'Completed':{
                        return true;
                    }

                    default: {
                        return false;
                    }
                }
            }

        };

        $scope.showChevron = function(job){
            switch(job.jobStatus){
                case 'Waiting': 
                case 'Pending':
                case 'Ready':{
                    return false;
                }
                default: {
                    if($scope.expanded === true){
                        return true;
                    }else{
                        return false;
                    }
                }
            }
        };

        $scope.isJonInState = function(job, status){
            if(job.jobStatus === status){
                return true;
            }else{
                return false;
            }
        };

        $scope.isJobReady = function(job){
            if (job.jobStatus === 'Ready') {
                return true;
            } else {
                return false;
            }
        };

        $scope.isJobPending = function (job) {
            if (job.jobStatus === 'Pending') {
                return true;
            } else {
                return false;
            }

        };
        $scope.isJobCompleted = function (job) {
            if ('Completed' === job.jobStatus) {
                return true;
            } else {
                return true;
            }
        };

        $scope.isJobFailed = function (job) {
            if (job.jobStatus === 'Failed') {
                return true;
            } else {
                return false;
            }
        };

        $scope.isJobRunning = function (job) {
            if (job.jobStatus === 'Running' || job.jobStatus === 'Pending') {
                return true;
            } else {
                return false;
            }
        };

        $scope.isJobScheduled = (job) => { 
            if (
                job.schedulingInfo &&
				job.schedulingInfo.scheduled
                ) {
                    
                    return job.schedulingInfo.scheduled;
                } else {
                    false;
                }
            }
        $scope.getScheduledMsg = (job) => {
             if (job.schedulingInfo) {
					return job.schedulingInfo.message != "" ? job.schedulingInfo.message :'Scheduled';
				} else {
                    return '';
				}
        }

        $scope.getActionsCount = function () {
            if ($scope.job.subJobs) {
                let count = 0;
                $scope.job.subJobs.forEach(element => {
                    if(element.jobStatus != 'Cancelled'){
                        count++;
                    }
                });
                // var idsString = $scope.job.inputs.ACTION_IDS;
                // var ids = JSON.parse(idsString);
                return count;//ids.length;
            } else {
                return '-';
            }
        };

        $scope.hasRight = function(){
            return AuthorizationUtility.checkAccessLevel(['INTERNAL_ADMIN', 'SUPER_ADMIN']);
        };

        $scope.hasSubjobs = function(job){
            if(job.subJobs != null && job.subJobs != undefined && job.subJobs.length > 0 ){
                return true;
            }else{
                return false;
            }
        }

        $scope.showScheduleTime = function(job) {
            if($scope.hasSubjobs(job) && ($scope.isJonInState(job, 'Completed') || $scope.isJonInState(job, 'Running') || $scope.isJonInState(job, 'Failed'))){
                return true;
            }
            else{
                return false;
            }
        }

        $scope.getJobMessage = (job) => {
            let errorMsg = $scope.getJobErrorMsg(job);
            if(errorMsg != ''){
                return errorMsg;
            }
            return $scope.getJobWarningMsg(job);
        }
        $scope.getJobMsgType = (job) => {
            let errorMsg = $scope.getJobErrorMsg(job);
            if(errorMsg != ''){
                return 'failed';
            }
            let warningMsg = $scope.getJobWarningMsg(job);
            if(warningMsg != ''){
                return 'warning';
            }
            return 'unknown';
        }

        $scope.getJobWarningMsg = (job) => {
            //  console.log('MSG =>',$scope.thejob.id);
            // return 'TEST 1234';
            // console.log('MSG =>',job.id, $scope.thejob.id, job.jobStatus);
            let msg = '';
            if(job.reports && job.reports.length > 0) {
                var json = job.reports[0].json.Payload;
                var obj = JSON.parse(json);
                // console.log(obj.ProcessAnalyzeWarningSummary.join());
                msg = obj.ProcessAnalyzeWarningSummary ? obj.ProcessAnalyzeWarningSummary.join() : '';
            }
            return msg;
        }
        $scope.getJobErrorMsg = (job) => {
            //  console.log('MSG =>',$scope.thejob.id);
            // return 'TEST 1234';
            // console.log('MSG =>',job.id, $scope.thejob.id, job.jobStatus);
            let msg = '';
            if(job.reports && job.reports.length > 0) {
                var json = job.reports[0].json.Payload;
                var obj = JSON.parse(json);
                // console.log(obj.ProcessAnalyzeWarningSummary.join());
                msg = obj.ProcessAnalyzeErrorSummary ? obj.ProcessAnalyzeErrorSummary.join() : '';
            }
            return msg;
        }

        init();

    }];
    return {
        restrict: 'E',
        transclude: false,
        scope: {
            job: '=', vm: '=', index: '='
        },
        controller: controller,
        templateUrl: "app/jobs/processing/job/import-jobs-row.component.html",
    };
}]);