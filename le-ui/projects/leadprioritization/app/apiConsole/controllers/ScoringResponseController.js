angular.module('pd.apiconsole.ScoringResponseController', [
    'mainApp.appCommon.utilities.ResourceUtility'
])

.directive('scoringResponse', function () {
    return {
        templateUrl: 'app/apiConsole/views/ScoringResponseView.html',
        controller: ['$scope', 'ResourceUtility', 'BrowserStorageUtility',
                     function ($scope, ResourceUtility) {
            $scope.ResourceUtility = ResourceUtility;
        }]
    };
});