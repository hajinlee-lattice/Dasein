angular.module('mainApp.setup.controllers.SaveAttributesModel', [
    'mainApp.appCommon.utilities.ResourceUtility'
])

.service('SaveAttributesModel', function ($compile, $http, ResourceUtility) {

    this.show = function ($parentScope) {
        $http.get('./app/setup/views/SaveAttributesConfirmView.html').success(function (html) {

            var scope = $parentScope.$new();

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

.controller('SaveAttributesController', function ($scope, ResourceUtility) {
    $scope.ResourceUtility = ResourceUtility;

    $scope.yesClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        $scope.$parent.saveAttributes();
        $("#modalContainer").modal('hide');
    };

    $scope.noClicked = function () {
        $("#modalContainer").modal('hide');
    };
});