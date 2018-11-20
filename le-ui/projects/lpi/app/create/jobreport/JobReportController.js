angular.module('lp.create.import.report', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'common.exceptions'
])
.controller('CSVReportController', function(
    $scope, JobsService, JobResult, ResourceUtility, 
    BrowserStorageUtility, ServiceErrorUtility
) {
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

    var clientSession = BrowserStorageUtility.getClientSession();

    $scope.TenantId = clientSession.Tenant.Identifier;
    $scope.AuthToken = BrowserStorageUtility.getTokenDocument();
    $scope.report = JobReport;
    $scope.data = JSON.parse(JobReport.json.Payload);
    var data = $scope.data;
    $scope.data.total_records = data.imported_records + data.ignored_records;
    $scope.errorlog = '/files/fileuploads/' + JobReport.name + '/import/errors' + 
        '?Authorization=' + $scope.AuthToken + 
        '&TenantId=' + $scope.TenantId;
    
    $scope.ResourceUtility = ResourceUtility;
});
