angular.module('mainApp.core.controllers.SalesforceCredentialController', [
    'mainApp.appCommon.utilities.ResourceUtility'
])

.controller('SalesforceCredentialController', function ($scope, $rootScope, $http, ResourceUtility) {
    $scope.ResourceUtility = ResourceUtility;
})

.directive('salesforceCredential', function () {
    return {
        scope: {
            logoLeftAlign: '=',
            productionComplete: '=',
            productionError: '=',
            productionSaveInProgress: '=',
            productionCredentials: '=',
            productionSaveButtonText: '@',
            productionSaveButtonClicked: '&',
            productionEditButtonText: '@',
            sandboxComplete: '=',
            sandboxError: '=',
            sandboxSaveInProgress: '=',
            sandboxCredentials: '=',
            sandboxSaveButtonText: '@',
            sandboxSaveButtonClicked: '&',
            sandboxEditButtonText: '@'
        },
        templateUrl: 'app/core/views/SalesforceCredentialTemplate.html'
    };
});