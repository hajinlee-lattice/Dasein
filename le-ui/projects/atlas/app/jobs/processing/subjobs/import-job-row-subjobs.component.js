import { MessagingFactory } from 'common/app/utilities/messaging-service';
import { NGPAJobsService } from '../pa-jobs.service';
import Observer from "common/app/http/observer";
import UrlUtils from "common/app/utilities/UrlUtils";
import Message, {
    MODAL,
    WARNING,
    CLOSE_MODAL
} from 'common/app/utilities/message';
angular.module('lp.jobs.row.subjobs', [])
    .factory('LeMessaging', MessagingFactory)
    .factory('NGPAJobsService', NGPAJobsService)
    .directive('importJobRowSubJobs', [function () {
        var controller = ['$scope', 'JobsStore', 'JobsService', 'BrowserStorageUtility', 'LeMessaging', 'NGPAJobsService', function ($scope, JobsStore, JobsService, BrowserStorageUtility, LeMessaging, NGPAJobsService) {
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
                $scope.watcher = $scope.$watchCollection('subjobs', function (newSubJobs) {
                    $scope.sobjobsGrouped = [];
                    $scope.usersGroupped = {};
                    $scope.groupdByUser(newSubJobs);
                    $scope.groupInsideUsers();
                });
            }

            this.$onDestroy = function () {
                if ($scope.watcher != null) {
                    $scope.watcher();
                }
            };

            function addToGroup(subjob) {
                if (!$scope.typesGroupd[subjob.jobType]) {
                    $scope.typesGroupd[subjob.jobType] = [];
                    $scope.typesGroupd[subjob.jobType].push(subjob);
                    subjob.actionsCount = 1;
                } else {
                    let actionsCount = $scope.typesGroupd[subjob.jobType][0].actionsCount + 1;
                    if (subjob.startTimestamp > $scope.typesGroupd[subjob.jobType][0].startTimestamp) {
                        $scope.typesGroupd[subjob.jobType].unshift(subjob);
                    } else {
                        $scope.typesGroupd[subjob.jobType].push(subjob);
                    }
                    $scope.typesGroupd[subjob.jobType][0].actionsCount = actionsCount;
                }
            }


            $scope.groupdByUser = function (subjobs) {
                subjobs.forEach(subjob => {
                    if (!$scope.usersGroupped[subjob.user]) {
                        $scope.usersGroupped[subjob.user] = [];
                        $scope.usersGroupped[subjob.user].push(subjob);
                    } else {
                        $scope.usersGroupped[subjob.user].push(subjob);
                    }
                });
            };

            $scope.groupInsideUsers = function () {
                const keys = Object.keys($scope.usersGroupped);
                for (const key of keys) {
                    let jobs = $scope.usersGroupped[key];
                    $scope.groupByTypes(jobs);
                }
            }

            $scope.groupByTypes = function (subjobs) {
                $scope.typesGroupd = {};
                if (subjobs && subjobs != null) {
                    subjobs.forEach(subjob => {
                        if (JobsStore.nonWorkflowJobTypes.indexOf(subjob.jobType) >= 0) {
                            addToGroup(subjob);
                        } else {
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
            
            $scope.getInputsValue = (subjob, key) => {
                // console.log(subjob.inputs, '  -- THE KEY ',key);
                let val = '';
                if(subjob.inputs){
                    val = subjob.inputs[key] ? subjob.inputs[key] : '';
                }
                return val;
            }
            $scope.getPrefixS3ActionType = (subjob) => {
                let ret =  $scope.getInputsValue(subjob, 'S3_IMPORT_EMAIL_FLAG') == 'true' ? 'S3 ' : '';
                return ret;
            }

            $scope.getActionType = function (subjob) {

                var type = subjob.jobType;
                switch (type) {
                    case 'cdlDataFeedImportWorkflow':
                        {
                            // console.log(subjob);
                            let prefixS3 = $scope.getPrefixS3ActionType(subjob);
                            return `${prefixS3}${'Import: '}`;
                        };
                    case 'cdlOperationWorkflow':
                        {
                            return 'Delete' + addEntityType(subjob) + ': ';
                        };
                    default:
                        {
                            return `${subjob.actionsCount > 1 ? subjob.actionsCount : ''} ${subjob.name}`;
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
                let fileNameEncoded = UrlUtils.encodeUrl(fileName);
                let filePathEncoded = UrlUtils.encodeUrl(filePath);
                let ret = path + fileNameEncoded + '&filePath=' + filePathEncoded + '&Authorization=' + auth;
                return ret;

            };

            $scope.canBeDownload = function (subjob) {
                var fileName = subjob.inputs != undefined ? subjob.inputs['SOURCE_FILE_NAME'] : '';
                if (fileName != '') {
                    var extPosition = fileName.lastIndexOf('.');
                    var extention = fileName.substring(extPosition, fileName.length).toLowerCase();
                    if (extention !== '.csv') {
                        return false;
                    } else {
                        return true;
                    }
                }
                return false;
            };

            $scope.getValidation = function (subjob) {
                if(subjob.jobStatus === 'Cancelled'){
                    return subjob.jobStatus;
                }
                if (subjob.jobStatus === 'Failed') {
                    return subjob.jobStatus;
                }
                if (subjob.jobStatus === 'Running') {
                    return 'In Progress';
                }
                if (getPayloadValue(subjob, 'total_rows') === getPayloadValue(subjob, 'imported_rows') && subjob.jobStatus === 'Completed') {
                    // console.log(subjob.jobStatus, ' == ',getPayloadValue(subjob, 'total_rows'), ' -- ',getPayloadValue(subjob, 'imported_rows'));
                    return 'Success';
                }
                if (getPayloadValue(subjob, 'imported_rows') < getPayloadValue(subjob, 'total_rows') && subjob.jobStatus === 'Completed' && (getPayloadValue(subjob, 'total_failed_rows') != getPayloadValue(subjob, 'total_rows'))) {
                    return 'Partial Success';
                }
                if (getPayloadValue(subjob, 'total_rows') === '-' && getPayloadValue(subjob, 'imported_rows') === '-' && subjob.jobStatus !== 'Completed') {
                    return 'In Progress';
                }
                if (!isNaN(getPayloadValue(subjob, 'total_failed_rows')) && !isNaN(getPayloadValue(subjob, 'total_rows')) && getPayloadValue(subjob, 'total_failed_rows') === getPayloadValue(subjob, 'total_rows') || (getPayloadValue(subjob, 'total_failed_rows') == getPayloadValue(subjob, 'total_rows'))) {
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

            $scope.hasImpactedEntity = function (subjob) {
                return subjob && subjob.outputs && JSON.parse(subjob.outputs.IMPACTED_BUSINESS_ENTITIES)[0] != undefined;
            }

            $scope.getErrorsLink = function (subjob) {
                var path = '/files/datafiles/errorscsv?filePath=';
                var filePath = subjob.outputs.DATAFEEDTASK_IMPORT_ERROR_FILES ? JSON.parse(subjob.outputs.DATAFEEDTASK_IMPORT_ERROR_FILES)[0] : '';
                var auth = BrowserStorageUtility.getTokenDocument();
                return path + filePath + '&Authorization=' + auth;
            }

            $scope.hasErrors = function (subjob) {
                return subjob.jobType == 'cdlDataFeedImportWorkflow' && subjob.outputs && subjob.outputs.DATAFEEDTASK_IMPORT_ERROR_FILES;
            }

            $scope.showWarning = function(subjob){
                let status = $scope.getValidation(subjob);
                // console.log(status);
                if(subjob.jobType =='cdlDataFeedImportWorkflow' && (subjob.errorMsg != null || status == 'Failed' || status == 'Partial Success')){
                    return true;
                }else{
                    return false;
                }
            }

            $scope.showCancel = function (subjob) {
                let status = this.getjobstatus();
                // console.log(status);
                if(subjob.jobType !='cdlDataFeedImportWorkflow' || (subjob.jobStatus === 'Running' || subjob.jobStatus === 'Pending')){
                    return false;
                }
                switch (status) {
                    case 'Ready':
                        return true;
                    default:
                        return false;
                }
            }
            $scope.getErrorMessage = function(subjob){
                if(subjob.errorMsg != null){ 
                     return subjob.errorMsg
                } else{
                    return "Please click on the number of Record Failed to check the error messages";
                }   
            }
            $scope.confirmCancelAction = function (subjob) {
                console.log(subjob);
                let name = $scope.getActionName(subjob);
                let type = $scope.getActionType(subjob);
                let p = '<span>Are you sure you want to cancel the action</span><br>';
                let n = `${'<p><strong>'}${type}${name}${'</strong>?</p>'}`;
                let msg = new Message(
                    'Test',
                    MODAL,
                    WARNING,
                    'Confirm',
                    `${p}${n}`
                );
                msg.setConfirmText('Yes, Cancel');
                msg.setDiscardText('No');
                msg.setIcon('fa fa-exclamation-circle');
                msg.setCallbackFn((args) => {
                    console.log(args);
                    let closeMsg = new Message([], CLOSE_MODAL);
                    closeMsg.setName(args.name);
                    // closeMsg.setCallbackFn();
                    LeMessaging.sendMessage(closeMsg);
                    if (args.action == "ok" && subjob.inputs.ACTION_ID) {
                        // console.log('Feature not connected to apis?', subjob);
                        let obs = new Observer(
                            response => {
                                console.log(response);
                                if(response.status == 200){
                                    subjob.jobStatus = 'Cancelled';
                                }
                                NGPAJobsService.unregister(obs);
                            },
                            error => {
                                console.error('ERROR');
                                NGPAJobsService.unregister(obs);
                            }
                        );
                        NGPAJobsService.cancelAction(subjob.inputs.ACTION_ID, obs)
                    }
                });
                LeMessaging.sendMessage(msg);
            }

            init();
        }];

        return {
            restrict: 'E',
            replace: true,
            scope: {
                subjobs: '=',
                applicationId: '=',
                getjobstatus: '&'
            },
            controller: controller,
            templateUrl: "app/jobs/processing/subjobs/import-job-row-subjobs.component.html",
        };
    }]);