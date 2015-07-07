angular.module('mainApp.models.modals.StaleModelModal', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.service('StaleModelModal', function ($compile, $rootScope, $http) {
    var self = this;
    this.show = function (modelId) {
        $http.get('./app/models/views/StaleModelConfirmView.html').success(function (html) {
            
            var scope = $rootScope.$new();
            scope.modelId = modelId;
            
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
.controller('StaleModelController', function ($scope, ResourceUtility) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.confirmClick = function () {
        $("#modalContainer").modal('hide');
    };
});
