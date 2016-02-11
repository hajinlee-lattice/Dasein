angular.module('pd.navigation.table', [

])

.controller('TableCtrl', function($scope, $rootScope) {
    $scope.tables = [
        {
            "name": "TOTAL JOBS",
            "items": {
                "Salesforce Imports": "11",
                "SF Import Frequency": "Daily",
                "Load File Imports": "3",
                "Model Creation": "17"
            }
        },{
            "name": "TOTAL ACCOUNTS",
            "items": {
                "Accounts": "234,567",
                "Matched": "180,123 (77)",
                "1+ Contact": "53,219 (23)",
                "Unique Accounts": "197,765"
            }
        },{
            "name": "TOTAL OPPORTUNITIES",
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
    ]
});