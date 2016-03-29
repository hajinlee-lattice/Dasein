angular.module('mainApp.create.csvReport', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.models.services.ModelService',
    'mainApp.core.utilities.NavUtility'
])
.controller('CSVReportController', [
    '$scope', 'JobsService', 'JobReport', 'ResourceUtility',
    function($scope, JobsService, JobReport, ResourceUtility) {
        if (!JobReport) {
            return;
        }
        
        $scope.report = JobReport;
        $scope.data = data = JSON.parse(JobReport.json.Payload);
        $scope.data.total_records = data.imported_records + data.ignored_records;
        $scope.errorlog = '/pls/fileuploads/' + JobReport.name.replace('_Report','') + '/import/errors';
        $scope.ResourceUtility = ResourceUtility;

        $scope.clickGetErrorLog = function($event) {
            JobsService.getErrorLog(JobReport).then(function(result) {
                var blob = new Blob([ result ], { type: "text/plain" }),
                    date = new Date(),
                    year = date.getFullYear(),
                    month = (1 + date.getMonth()).toString(),
                    month = month.length > 1 ? month : '0' + month,
                    day = date.getDate().toString(),
                    day = day.length > 1 ? day : '0' + day,
                    filename = 'import_errors.' + year + month + day + '.csv';
                
                saveAs(blob, filename);
            }, function(reason) {
                alert('Failed: ' + reason);
            });
        }
    }
]);