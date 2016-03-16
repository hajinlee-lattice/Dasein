angular.module('mainApp.setup.controllers.DiscardEditFieldsModel', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.setup.utilities.SetupUtility'
])

.service('DiscardEditFieldsModel', function ($compile, $templateCache, $rootScope, $http, ResourceUtility) {

    this.show = function ($manageFieldsScope) {
        $http.get('app/setup/views/DiscardEditFieldsView.html', { cache: $templateCache }).success(function (html) {

            var scope = $rootScope.$new();
            scope.manageFieldsScope = $manageFieldsScope;

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

.controller('DiscardEditFieldsController', function ($scope, $rootScope, ResourceUtility, SetupUtility, ModelService) {
    $scope.ResourceUtility = ResourceUtility;

    $scope.yesClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        $scope.manageFieldsScope.discardAllChanges();
        $("#modalContainer").modal('hide');
    };

    $scope.noClicked = function () {
        $("#modalContainer").modal('hide');
    };
});