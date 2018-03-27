angular.module('lp.jobs.row.subjobs', [])

    .directive('importJobRowSubJobs', [function () {
        var controller = ['$scope', 'JobsStore', 'JobsService', 'BrowserStorageUtility', function ($scope, JobsStore, JobsService, BrowserStorageUtility) {
            function init() {
                // console.log('EXPANDED ======= ',$scope);
            }
            $scope.getActionType = function (subjob) {
                
                var type = subjob.jobType;
                switch (type) {
                    case 'cdlDataFeedImportWorkflow': {
                        return 'Import: ';
                    };
                    case 'cdlOperationWorkflow': {
                        return 'Delete: ';
                    };
                    default: {
                        return subjob.name;
                    }
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
            }

            $scope.getValidation = function (subjob) {
                if(subjob.jobStatus === 'Completed'){
                    return subjob.jobStatus;
                }
                if(subjob.jobStatus === 'Failed'){
                    return 'Failed';
                }
                if(subjob.jobStatus === 'Running'){
                    return 'In Progress';
                }
                
                var recordFound = $scope.getRecordFound(subjob);
                var recordUploaded = $scope.getRecordUploaded(subjob);
                if (recordFound === '-' && recordUploaded === '-') {
                    return 'In Progress'
                }
                if (recordFound > 0 && recordUploaded == 0) {
                    return 'Failed';
                }
                if (recordFound === recordUploaded) {
                    return 'Success';
                }
                if (recordFound != recordUploaded) {
                    return 'Partial Success';
                }
                return subjob.jobStatus;


            }
            $scope.getRecordFound = function (subjob) {
                if (subjob.reports && subjob.reports.length > 0) {
                    var json = subjob.reports[0].json.Payload;
                    var obj = JSON.parse(json);
                    return obj.total_rows;
                } else {
                    return '-';
                }
            }
            $scope.getRecordFailed = function (subjob) {
                if (subjob.reports && subjob.reports.length > 0) {
                    var json = subjob.reports[0].json.Payload;
                    var obj = JSON.parse(json);
                    return obj.ignored_rows;
                } else {
                    return '-';
                }
            }
            $scope.getRecordUploaded = function (subjob) {
                if (subjob.reports && subjob.reports.length > 0) {
                    var json = subjob.reports[0].json.Payload;
                    var obj = JSON.parse(json);
                    return obj.imported_rows;
                } else {
                    return '-';
                }
            }
            $scope.getUser = function (subjob) {
                return subjob.user;
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

