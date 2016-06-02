angular.module('mainApp.sfdc.sfdcCredentials', [
    'mainApp.core.utilities.BrowserStorageUtility'
])
.controller('sfdcCredentialsController', function($scope, $q, $http, BrowserStorageUtility) {
        $scope.showTokenMessage = false;
        $scope.loading = false;

        $scope.generateAndEmailSFDCAccessTokenClicked = function() {
            $scope.loading = true;
            $scope.showTokenMessage = false;

            var clientSession = BrowserStorageUtility.getClientSession();
            var emailAddress = clientSession.EmailAddress;
            var tenantId = clientSession.Tenant.Identifier;

            $http({
                method: 'GET',
                url: '/pls/bisaccesstoken',
                params: {
                    username: emailAddress,
                    tenantId: tenantId
                },
                headers: {
                    'Content-Type': 'application/json'
                }
            })
            .success(function(data, status, headers, config) {
                $scope.showTokenMessage = true;
                $scope.tokenSucceeded = true;
                $scope.loading = false;
            })
            .error(function(data, status, headers, config) {
                $scope.showTokenMessage = true;
                $scope.tokenSucceeded = false;
                $scope.loading = false;
            });
        };

        $scope.closeSFDCMessage = function() {
            $scope.showTokenMessage = false;
        };
    }
);
