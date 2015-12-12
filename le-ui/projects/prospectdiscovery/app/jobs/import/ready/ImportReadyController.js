angular.module('pd.jobs.import.ready', [

])
.controller('ImportReadyController', function($scope, $rootScope, $stateParams, ImportReadyService) {
    $scope.jobId = $stateParams.jobId;
    
    ImportReadyService.getImportSummaryForJobId($scope.jobId).then(function(result) {
        var importSummary = result.resultObj;
        $scope.tables = [
            {
                "name": "ACCOUNTS",
                "items": {
                    "Accounts": importSummary.accounts.total,
                    "Matched": importSummary.accounts.total * importSummary.accounts.match_rate,
                    "1+ Contact": importSummary.accounts.with_contacts,
                    "Unique Accounts": importSummary.accounts.unique
                }
            },{
                "name": "OPPORTUNITIES",
                "items": {
                    "Opportunities": importSummary.opportunities.total,
                    "Closed-Won": importSummary.opportunities.closed_won,
                    "Closed": importSummary.opportunities.closed
                }
            },{
                "name": "OTHER INFO",
                "items": {
                    "Contacts": importSummary.contacts.total,
                    "Leads": importSummary.leads.total,
                    "Data Range": importSummary.accounts.date_range.begin + " - " + importSummary.accounts.date_range.end
                }
            }
        ];
    });
})

.service('ImportReadyService', function($http, $q, _) {
    var importSummary = {
        "accounts": {
            "date_range":{
                "begin":"10/01/2013",
                "end":"11/01/2014"
            },
            "total": 1000000,
            "match_rate": 0.05,
            "with_contacts": 2000,
            "unique": 300
        },
        "contacts": {
            "total": 2000
        },
        "leads": {
            "total": 3000
        },
        "opportunities": {
            "total": 10000,
            "closed_won": 2000,
            "closed": 1000
        }
    };
    
    this.getImportSummaryForJobId = function(jobId) {
        var deferred = $q.defer();
        var result;
        
        /**
        $http({
            method: 'GET',
            url: '/pls/importsummary'
        }).then(
            function onSuccess(response) {
                var jobs = response.data;
                result = {
                    success: true,
                    resultObj: null
                };
            }
        )
        */
                
        result = {
            success: true,
            resultObj: null
        };
        result.resultObj = importSummary;

        deferred.resolve(result);
        return deferred.promise;
    };
});