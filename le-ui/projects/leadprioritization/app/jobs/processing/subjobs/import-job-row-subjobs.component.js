angular.module('lp.jobs.row.subjobs', [])

    .directive('importJobRowSubJobs', [function () {
        var controller = ['$scope', 'JobsStore', 'BrowserStorageUtility', function ($scope, JobsStore, BrowserStorageUtility) {
            function init() {
                // console.log('EXPANDED ======= ',$scope);
            }
            $scope.getActionType = function (subjob) {
                var type = subjob.jobType;
                switch(type){
                    case 'cdlDataFeedImportWorkflow' : {
                        return 'Import';
                    };
                    case 'cdlOperationWorkflow':{
                        return 'Delete';
                    };
                    case 'metadataChange':{
                        return 'Metadata Change';
                    };
                    default: {
                        return 'Unknown';
                    }
                }
            }
            $scope.getActionName = function (subjob) {
                if (subjob.inputs && subjob.inputs != null) {
                    return subjob.inputs['SOURCE_DISPLAY_NAME'];
                }
            }
            $scope.getActionLink = function (subjob) {
                if (subjob.inputs && subjob.inputs != null) {
                    var appId = $scope.applicationId;
                    var fileName = subjob.inputs['SOURCE_FILE_NAME'];
                    var auth = BrowserStorageUtility.getTokenDocument();
                    var clientSession = BrowserStorageUtility.getClientSession();
                    var tenantId = clientSession.Tenant.Identifier;
                    // {{pls}}/datafiles/sourcefile?fileName=<internalFileName of the file>
                    var ret = '/files/datafiles/sourcefilecsv/'+appId+'?fileName='+fileName+'&Authorization='+auth+'&TenantId='+tenantId;
                    ret = 'pls/datafiles/sourcefile?filename='+fileName;
                    // /files/datafiles/sourcefilecsv/{{job.applicationId}}?fileName={{job.source}}&Authorization={{auth}}&TenantId={{TenantId}}
                    return ret;//subjob.inputs['SOURCE_FILE_NAME'];
                }
            }

            $scope.getValidation = function (subjob) {
                var recordFound = $scope.getRecordFound(subjob);
                var recordUploaded = $scope.getRecordUploaded(subjob);
                if(recordFound === '-' && recordUploaded === '-'){
                    return 'In Progress'
                }
                if(recordFound > 0 && recordUploaded == 0){
                    return 'Failed';
                }
                if(recordFound === recordUploaded){
                    return 'Success';
                }
                if(recordFound != recordUploaded){
                    return 'Partial Success';
                }
                
                
                
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

