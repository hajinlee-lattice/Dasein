angular.module('mainApp.setup.controllers.ClearDeploymentModel', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.setup.services.TenantDeploymentService'
])

.service('ClearDeploymentModel', function ($compile, $templateCache, $rootScope, $http) {

    this.show = function () {
        $http.get('app/setup/views/ClearDeploymentConfirmView.html', { cache: $templateCache }).success(function (html) {

            var modalElement = $("#modalContainer");
            var scope = $rootScope.$new();
            $compile(modalElement.html(html))(scope);

            var options = {
                backdrop: "static"
            };
            modalElement.modal(options);
            modalElement.modal('show');

            // Remove the created HTML from the DOM
            modalElement.on('hidden.bs.modal', function (evt) {
                modalElement.empty();
            });
        });
    };

})

.controller('ClearDeploymentController', function ($scope, $rootScope, ResourceUtility, NavUtility, TenantDeploymentService) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.clearInProgress = false;

    $scope.yesClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        if ($scope.clearInProgress) { return; }
        $scope.clearInProgress = true;
        $scope.showClearError = false;
        TenantDeploymentService.DeleteTenantDeployment().then(function (result) {
            if(result.Success === true) {
                $("#modalContainer").modal('hide');
                $rootScope.$broadcast(NavUtility.DEPLOYMENT_WIZARD_NAV_EVENT);
            } else {
                $rootScope.clearErrorMessage = result.ResultErrors;
                $scope.showClearError = true;
            }
            $scope.clearInProgress = false;
        });
    };

    $scope.noClicked = function () {
        $("#modalContainer").modal('hide');
    };
});