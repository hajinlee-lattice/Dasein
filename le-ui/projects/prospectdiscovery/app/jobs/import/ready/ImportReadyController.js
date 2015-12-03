angular.module('controllers.jobs.import.ready', [

])
.controller('ImportReadyController', function($scope, $rootScope, $stateParams) {
    $scope.jobId = $stateParams.jobId;
    
    $scope.completionTimes = { "load_data": "1449132082493", "match_data": "1449132082493",
            "generate_insights": "1449132082493", "create_model": "1449132082493", "create_global_target_market": "1449132082493" };
            
    
    $scope.tables = [
        {
            "name": "ACCOUNTS",
            "items": {
                "Accounts": "234,567",
                "Matched": "180,123 (77)",
                "1+ Contact": "53,219 (23)",
                "Unique Accounts": "197,765"
            }
        },{
            "name": "OPPORTUNITIES",
            "items": {
                "Opportunities": "17,890",
                "Closed-Won": "1,234",
                "Closed": "1,234"
            }
        },{
            "name": "OTHER INFO",
            "items": {
                "Contacts": "167,890",
                "Leads": "456,789",
                "Data Range": "10/2013 - 10/2015"
            }
        }
    ];
});