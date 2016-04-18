angular.module('pd.apiconsole.ScoringResponseController', [
    'mainApp.appCommon.utilities.ResourceUtility'
])

.directive('scoringResponse', function () {
    return {
        templateUrl: 'app/apiConsole/views/ScoringResponseView.html',
        controller: ['$scope', 'ResourceUtility', 'BrowserStorageUtility',
                     function ($scope, ResourceUtility) {
            $scope.ResourceUtility = ResourceUtility;

            $scope.scoringRequested = false;
            $scope.scoreRecordLoading = false;
            $scope.scoringRequestError = "This is a mockup data in local.";
            $scope.score = 82;
            var json = {id: "string",score: 0,timestamp: "string",warnings: [{"warning": "string","warning_description": "string"}]};
            $scope.jsonData = JSON.stringify(json, null, "    ");
            $scope.timeElapsed = "50 MS";
        }]
    };
});