angular.module('lp.create.import.report', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.modules.ServiceErrorModule'
])
.controller('CSVReportController', function($scope, JobsService, JobResult, ResourceUtility, ServiceErrorUtility) {
    var reports = JobResult.reports,
        JobReport = null;

    reports.forEach(function(item) {
        if (item.purpose == "IMPORT_DATA_SUMMARY") {
            JobReport = item;
        }
    });

    if (!JobReport) {
        return;
    }
    
    JobReport.name = JobReport.name.substr(0, JobReport.name.indexOf('.csv') + 4);

    $scope.report = JobReport;
    $scope.data = data = JSON.parse(JobReport.json.Payload);
    $scope.data.total_records = data.imported_records + data.ignored_records;
    $scope.errorlog = '/pls/fileuploads/' + JobReport.name + '/import/errors';
    $scope.ResourceUtility = ResourceUtility;

    ServiceErrorUtility.process({ data: JobResult });

    $scope.clickGetErrorLog = function($event) {
        JobsService.getErrorLog(JobReport, JobResult.jobType).then(function(result) {
            var blob = new Blob([ result ], { type: "application/csv" }),
                date = new Date(),
                year = date.getFullYear(),
                month = (1 + date.getMonth()).toString(),
                month = month.length > 1 ? month : '0' + month,
                day = date.getDate().toString(),
                day = day.length > 1 ? day : '0' + day,
                filename = 'import_errors.' + year + month + day + '.csv';
            
            saveAs(blob, filename);
        });
    }
});
