angular.module('lp.jobs.row.subjobs', [])

    .directive('importJobRowSubJobs', [function () {
        var controller = ['$scope', 'JobsStore', 'JobsService', 'BrowserStorageUtility', function ($scope, JobsStore, JobsService, BrowserStorageUtility) {
            $scope.typesGroupd = {};
            $scope.watcher = null;
            function init() {
                // console.log('EXPANDED ======= ',$scope);
                $scope.emptyMessage = "No Actions Found";
                $scope.sobjobsGrouped = [];
                $scope.usersGroupped = {};
                $scope.typesGroupd = {};
                $scope.groupdByUser($scope.subjobs);
                $scope.groupInsideUsers();
                $scope.watcher = $scope.$watchCollection('subjobs', function(newSubJobs) {
                    $scope.sobjobsGrouped = [];
                    $scope.usersGroupped = {};
                    $scope.groupdByUser(newSubJobs);
                    $scope.groupInsideUsers();
                });
            }

            
            function addToGroup(subjob){
                if(!$scope.typesGroupd[subjob.jobType]){
                    $scope.typesGroupd[subjob.jobType] = [];
                    $scope.typesGroupd[subjob.jobType].push(subjob);
                    subjob.actionsCount = 1;
                }else{
                    let actionsCount = $scope.typesGroupd[subjob.jobType][0].actionsCount + 1;
                    if(subjob.startTimestamp > $scope.typesGroupd[subjob.jobType][0].startTimestamp){
                        $scope.typesGroupd[subjob.jobType].unshift(subjob);
                    }else{
                        $scope.typesGroupd[subjob.jobType].push(subjob);
                    }
                    $scope.typesGroupd[subjob.jobType][0].actionsCount = actionsCount;
                }
            }
            
            this.$onDestroy = function () {
                if($scope.watcher != null){
                    $scope.watcher();
                }
            };

            $scope.groupdByUser = function(subjobs){
                subjobs.forEach(subjob => {
                    if(!$scope.usersGroupped[subjob.user]){
                        $scope.usersGroupped[subjob.user] = [];
                        $scope.usersGroupped[subjob.user].push(subjob);
                    }else{
                        $scope.usersGroupped[subjob.user].push(subjob);
                    }
                });
            };

            $scope.groupInsideUsers = function(){
                const keys = Object.keys($scope.usersGroupped);
                for(const key of keys){
                    let jobs = $scope.usersGroupped[key];
                    $scope.groupByTypes(jobs);
                }
            }

            $scope.groupByTypes = function(subjobs){
                $scope.typesGroupd = {};
                if(subjobs && subjobs != null){
                    subjobs.forEach(subjob => {
                        if(JobsStore.nonWorkflowJobTypes.indexOf(subjob.jobType) >=0){
                            addToGroup(subjob);
                        }else{
                            $scope.sobjobsGrouped.push(subjob);
                        }
                    });
                    const keys = Object.keys($scope.typesGroupd);
                    for (const key of keys) {
                        let array = $scope.typesGroupd[key];
                        $scope.sobjobsGrouped.push(array[0]);
                    }
                }
            };

            $scope.getActionType = function (subjob) {

                var type = subjob.jobType;
                switch (type) {
                    case 'cdlDataFeedImportWorkflow':
                        {
                            return 'Import: ';
                        };
                    case 'cdlOperationWorkflow':
                        {
                            return 'Delete' + addEntityType(subjob) + ': ';
                        };
                    default:
                        {
                            return `${subjob.actionsCount > 1 ? subjob.actionsCount: ''} ${subjob.name}`;
                        }
                }
            }

            function addEntityType(subjob) {
                if ($scope.canBeDownload(subjob) && subjob.outputs.IMPACTED_BUSINESS_ENTITIES) {
                    return ' ' + JSON.parse(subjob.outputs.IMPACTED_BUSINESS_ENTITIES)[0] + '(s)';
                } else {
                    return '';
                }
            }
            $scope.getActionName = function (subjob) {
                if (subjob.inputs && subjob.inputs != null) {
                    var ret = subjob.inputs['SOURCE_DISPLAY_NAME'];
                    return ret;
                }
            }

            $scope.getDownloadLink = function (subjob) {
                var path = '/files/datafiles/sourcefile?fileName=';
                var fileName = subjob.inputs != undefined ? subjob.inputs['SOURCE_FILE_NAME'] : '';
                var filePath = subjob.inputs != undefined ? (subjob.inputs['SOURCE_FILE_PATH'] != undefined ? subjob.inputs['SOURCE_FILE_PATH'] : '') : '';
                var auth = BrowserStorageUtility.getTokenDocument();
                var clientSession = BrowserStorageUtility.getClientSession();
                var tenantId = clientSession.Tenant.Identifier;
                return path + fileName + '&filePath=' + filePath + '&Authorization=' + auth;
            };

            $scope.canBeDownload = function(subjob) {
                var fileName = subjob.inputs != undefined ? subjob.inputs['SOURCE_FILE_NAME'] : '';
                if(fileName != ''){
                    var extPosition = fileName.lastIndexOf('.');
                    var extention = fileName.substring(extPosition, fileName.length).toLowerCase();
                    if(extention !== '.csv'){
                        return false;
                    }else{
                        return true;
                    }
                }
                return false;
            };

            $scope.getValidation = function (subjob) {
                if (subjob.jobStatus === 'Failed') {
                    return 'Failed';
                }
                if (subjob.jobStatus === 'Running') {
                    return 'In Progress';
                }
                if (getPayloadValue(subjob, 'total_rows') === getPayloadValue(subjob, 'imported_rows') && subjob.jobStatus === 'Completed') {
                    // console.log(subjob.jobStatus, ' == ',getPayloadValue(subjob, 'total_rows'), ' -- ',getPayloadValue(subjob, 'imported_rows'));
                    return 'Success';
                }
                if (getPayloadValue(subjob, 'imported_rows') < getPayloadValue(subjob, 'total_rows') && subjob.jobStatus === 'Completed') {
                    return 'Partial Success';
                }
                if (getPayloadValue(subjob, 'total_rows') === '-' && getPayloadValue(subjob, 'imported_rows') === '-' && subjob.jobStatus !== 'Completed') {
                    return 'In Progress';
                }
                if (!isNaN(getPayloadValue(subjob, 'total_failed_rows')) && !isNaN(getPayloadValue(subjob, 'total_rows')) && getPayloadValue(subjob, 'total_failed_rows') === getPayloadValue(subjob, 'total_rows')) {
                    return 'Failed';
                }
                return subjob.jobStatus;


            };

            function getPayloadValue(subjob, field) {
                if (subjob.reports && subjob.reports.length > 0) {
                    var json = subjob.reports[0].json.Payload;
                    var obj = JSON.parse(json);
                    var ret = obj[field] != undefined ? obj[field] : '-';
                    return ret;
                } else {
                    return '-';
                }
            }

            $scope.getRecordFound = function (subjob) {
                if ($scope.hasImpactedEntity()) {
                    return getPayloadValue(subjob, JSON.parse(subjob.outputs.IMPACTED_BUSINESS_ENTITIES)[0] + '_Deleted');
                } else {
                    return getPayloadValue(subjob, 'total_rows');
                }
            }
            $scope.getRecordFailed = function (subjob) {
                return getPayloadValue(subjob, 'total_failed_rows');

            }
            $scope.getRecordUploaded = function (subjob) {
                return getPayloadValue(subjob, 'imported_rows');

            }
            $scope.getUser = function (subjob) {
                return subjob.user;
            }

            $scope.hasImpactedEntity = function(subjob) {
                return subjob && subjob.outputs && JSON.parse(subjob.outputs.IMPACTED_BUSINESS_ENTITIES)[0] != undefined;
            }

            $scope.getErrorsLink = function (subjob) {
                var path = '/files/datafiles/errorscsv?filePath=';
                var filePath = subjob.outputs.DATAFEEDTASK_IMPORT_ERROR_FILES ? JSON.parse(subjob.outputs.DATAFEEDTASK_IMPORT_ERROR_FILES)[0] : '';
                var auth = BrowserStorageUtility.getTokenDocument();
                return path + filePath + '&Authorization=' + auth;
            }

            $scope.hasErrors = function(subjob) {
                return subjob.jobType == 'cdlDataFeedImportWorkflow' && subjob.outputs && subjob.outputs.DATAFEEDTASK_IMPORT_ERROR_FILES;
            }

            init();
        }];

        return {
            restrict: 'E',
            replace: true,
            scope: {
                subjobs: '=',
                applicationId: '='
            },
            controller: controller,
            templateUrl: "app/jobs/processing/subjobs/import-job-row-subjobs.component.html",
        };
    }]);