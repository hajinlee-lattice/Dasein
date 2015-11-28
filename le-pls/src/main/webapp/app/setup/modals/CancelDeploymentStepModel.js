angular.module('mainApp.setup.controllers.CancelDeploymentStepModel', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility'
])

.service('CancelDeploymentStepModel', function ($compile, $rootScope, $http, ResourceUtility) {

    this.show = function ($parentScope, link) {
        $http.get('./app/setup/views/CancelDeploymentStepConfirmView.html').success(function (html) {

            var scope = $rootScope.$new();
            scope.parentScope = $parentScope;
            scope.link = link;

            var modalElement = $("#modalContainer");
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

.controller('CancelDeploymentStepController', function ($scope, $rootScope, ResourceUtility) {
    $scope.ResourceUtility = ResourceUtility;

    if ($scope.step === "IMPORT_SFDC_DATA") {
        $scope.confirmContent = ResourceUtility.getString('SETUP_CANCEL_IMPORT_SFDC_DATA_LABEL');
    } else if ($scope.step === "ENRICH_DATA") {
        $scope.confirmContent = ResourceUtility.getString('SETUP_CANCEL_ENRICH_DATA_LABEL');
    }

    $scope.yesClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        $scope.parentScope.cancelStep($scope.link);
        $("#modalContainer").modal('hide');
    };

    $scope.noClicked = function () {
        $("#modalContainer").modal('hide');
    };
});