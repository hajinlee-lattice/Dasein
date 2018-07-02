angular.module('lp.jobs.import.row', ['mainApp.appCommon.directives.modal.window'])

    .directive('importJobRow', [function () {
        var controller = ['$scope', '$q', '$timeout', 'JobsStore', 'ModalStore', 'AuthorizationUtility', 'ServiceErrorUtility', function ($scope, $q, $timeout, JobsStore, ModalStore, AuthorizationUtility, ServiceErrorUtility) {
            $scope.thejob = $scope.job;
            $scope.disableButton = false;
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
                if (action && action.action === 'run') {
                    if(action.obj){
                        action.obj.jobStatus = 'Waiting';
                    }
                    $scope.disableButton = true;
                    JobsStore.runJob($scope.job).then(function (result) {

                        $scope.disableButton = true;
                        if(result.Success === true && action.obj) {
                            action.obj.jobStatus = 'Pending';
                        } else {
                            var errorMsg = result.errorMsg;
                            ServiceErrorUtility.showBanner({data: {errorMsg: errorMsg}});
                        }
                        
                    });
                }else{
                    $scope.disableButton = false;
                }
            }
            
            function init() {

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

            $scope.getSubJobsPartialSuccess = function () {
                var listPartialSuccess = [];
                for (var i = 0; i < $scope.job.subJobs.length; i++) {
                    var found = getRecordFound($scope.job.subJobs[i]);
                    var uploaded = getRecordUploaded($scope.job.subJobs[i]);
                    if (found != uploaded && $scope.job.subJobs[i].inputs != undefined) {
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
                        if (job.jobStatus === 'Completed') {
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

            $scope.vm.run = function (job) {
                // $scope.clicked = true;
                var show = $scope.showWarningRun(job);
                if (show) {
                    $scope.vm.toggleModal(job);
                } else {
                    // var obj = JSON.stringify(job);
                    $scope.vm.callback({ 'action': 'run', 'obj': job });
                }
            };

            $scope.showWarningRun = function (job) {
                var subJobs = job.subJobs;

                var allCompleted = true;
                if (subJobs) {
                    for (var i = 0; i < subJobs.length; i++) {
                        if (subJobs[i].jobStatus === 'Running') {
                            allCompleted = false;
                            break;
                        }
                    }
                }
                return !allCompleted;
            };

            $scope.showScheduleTime = function(job){
                if(!$scope.disableRunButton(job) && $scope.showRunButton(job)){
                    return true;
                }else{
                    return false;
                }
            };
            
            $scope.mouseDownRun  = function(job){
                var formerFailed = $scope.vm.isLastOneFailed();
                $scope.vm.callback = callbackModalWindow;
                $scope.disableButton = true;
                
                if(formerFailed === true){
                    var someIncompleted = $scope.showWarningRun(job);
                    if(someIncompleted){
                        $scope.vm.msgUrl = 'app/jobs/processing/actions.failed.incomplete.message.html';
                    }else{
                        $scope.vm.msgUrl = 'app/jobs/processing/actions.failed.message.html';
                    }
                    $scope.vm.toggleModal(job);
                }else{
                    $scope.vm.run(job);
                }
            };

            $scope.getJobStatus = function(job){
                return job.jobStatus;
            
            };

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
                switch(job.jobStatus){
                    case 'Failed':
                    case 'Completed':
                    case 'Pending':
                    case 'Running':{
                        return false;
                    }
                    default: {
                        return true;
                    }
                }
            };

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

            $scope.getActionsCount = function () {
                if ($scope.job.subJobs) {
                    // var idsString = $scope.job.inputs.ACTION_IDS;
                    // var ids = JSON.parse(idsString);
                    return $scope.job.subJobs.length;//ids.length;
                } else {
                    return '-';
                }
            };

            $scope.hasRight = function(){
                return AuthorizationUtility.checkAccessLevel(['INTERNAL_ADMIN', 'SUPER_ADMIN']);
            };

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

