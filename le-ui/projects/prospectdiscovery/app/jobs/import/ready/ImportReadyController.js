angular.module('pd.jobs.import.ready', [
    'pd.jobs'
])
.controller('ImportReadyController', function($scope, $rootScope, $stateParams, JobsService, ImportReadyService) {
    $scope.jobId = $stateParams.jobId;

    $scope.jobType;
    $scope.jobStartDate;
    $scope.jobStartTime;
    $scope.user;
    $scope.stepsCompletedTimes;
    
    JobsService.getJobStatus($scope.jobId).then(function(result) {
        var jobStatus = result.resultObj;
        $scope.jobType = jobStatus.jobType;
        $scope.user = jobStatus.user;
        var jobStartTimeStamp = new Date(jobStatus.startTimestamp);
        $scope.jobStartDate = jobStartTimeStamp.getMonth() + '/' + jobStartTimeStamp.getDate() + '/' + jobStartTimeStamp.getFullYear();
        $scope.jobStartTime = jobStartTimeStamp.toLocaleTimeString();
        $scope.stepsCompletedTimes = jobStatus.completedTimes;
    });
    
    ImportReadyService.getImportSummaryForJobId($scope.jobId).then(function(result) {
        var importSummary = result.resultObj.json.Payload;
        if (importSummary.indexOf("NaN") > -1) {
            importSummary = importSummary.replace("NaN", null);
        }
        importSummary = $.parseJSON(importSummary);
        var matchRate, withContactRate;
        var numMatched;
        if (! importSummary.accounts.match_rate) {
            matchRate = "-";
            numMatched = "-";
        } else {
            matchRate = Math.round(importSummary.accounts.match_rate * 100) + "%";
            numMatched = Math.round(importSummary.accounts.match_rate * importSummary.accounts.total);
        }
        
        var startDate = new Date(importSummary.date_range.begin);
        var endDate = new Date(importSummary.date_range.end);
        $scope.tables = [
            {
                "name": "ACCOUNTS",
                "items": {
                    "Accounts": importSummary.accounts.total,
                    "Matched": numMatched + "(" + matchRate + ")",
                    "1+ Contact": importSummary.accounts.with_contacts + "(" +
                        Math.round(importSummary.accounts.with_contacts / importSummary.accounts.total * 100) + "%)",
                    "Unique Accounts": importSummary.accounts.unique
                }
            },{
                "name": "OPPORTUNITIES",
                "items": {
                    "Opportunities": importSummary.accounts.with_opportunities,
                    "Closed-Won": importSummary.leads.closed_won,
                    "Closed": importSummary.leads.closed
                }
            },{
                "name": "OTHER INFO",
                "items": {
                    "Contacts": importSummary.contacts.total,
                    "Leads": importSummary.leads.total,
                    "Data Range": startDate.getMonth() + "/" + startDate.getFullYear() + " - "
                        + endDate.getMonth() + "/" + endDate.getFullYear()
                }
            }
        ];
    });
})

.service('ImportReadyService', function($http, $q, _) {
    this.getImportSummaryForJobId = function(jobId) {
        var deferred = $q.defer();
        var result = {
            success: true,
            resultObj: null
        }
        
        $http({
            method: 'GET',
            url: '/pls/targetmarkets/default'
        }).then(
            function onSuccess(response) {
                var targetmarket = response.data;
                var reportName;
                if (targetmarket.reports.length == 1) {
                    reportName = targetmarket.reports[0].report_name;

                    $http({
                        method: 'Get',
                        url: '/pls/reports/' + reportName
                    }).then(
                       function onSuccess(response) {
                           result.resultObj = response.data;
                           deferred.resolve(result);
                       }, function onError(response) {
                           
                       }
                    )
                }
            }, function onError(response) {
                
            }
        )

        return deferred.promise;
    };
});
