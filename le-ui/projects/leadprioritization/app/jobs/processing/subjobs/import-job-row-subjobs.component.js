angular.module('lp.jobs.row.subjobs', [])

    .directive('importJobRowSubJobs', [function () {
        var controller = ['$scope', 'JobsStore', 'JobsService', 'BrowserStorageUtility', function ($scope, JobsStore, JobsService, BrowserStorageUtility) {
            function init() {
                // console.log('EXPANDED ======= ',$scope);
                $scope.emptyMessage = "No Actions Found";
            }
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
                            return subjob.name;
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
                var auth = BrowserStorageUtility.getTokenDocument();
                var clientSession = BrowserStorageUtility.getClientSession();
                var tenantId = clientSession.Tenant.Identifier;
                return path + fileName + '&Authorization=' + auth;
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

                if (getPayloadValue(subjob, 'total_rows') === '-' && getPayloadValue(subjob, 'imported_rows') === '-' && subjob.jobStatus !== 'Completed') {
                    return 'In Progress';
                }
                if (!isNaN(getPayloadValue(subjob, 'total_failed_rows')) && !isNaN(getPayloadValue(subjob, 'total_rows')) && getPayloadValue(subjob, 'total_failed_rows') === getPayloadValue(subjob, 'total_rows')) {
                    return 'Failed';
                }
                if (getPayloadValue(subjob, 'total_rows') === getPayloadValue(subjob, 'imported_rows') && subjob.jobStatus === 'Completed') {
                    return 'Success';
                }
                if (getPayloadValue(subjob, 'imported_rows') < getPayloadValue(subjob, 'total_rows') && subjob.jobStatus === 'Completed') {
                    return 'Partial Success';
                }
                return subjob.jobStatus;


            }

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