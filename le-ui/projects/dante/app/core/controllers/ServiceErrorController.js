angular.module('mainApp.core.controllers.ServiceErrorController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.ServiceErrorUtility'
])
.controller('ServiceErrorController', function ($scope, ResourceUtility, ServiceErrorUtility) {
    
    $scope.serviceErrorTitle = ResourceUtility.getString('UNEXPECTED_SERVICE_ERROR_TITLE');
    if ($scope.serviceErrorTitle == "UNEXPECTED_SERVICE_ERROR_TITLE") {
        $scope.serviceErrorTitle = "System error occurred. Please contact Lattice support with the error below.";
    }
    
    $scope.serviceErrorMessage = ServiceErrorUtility.ServiceResultError;
});