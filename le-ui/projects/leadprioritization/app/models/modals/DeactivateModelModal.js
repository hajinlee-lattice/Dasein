angular.module('mainApp.models.modals.DeactivateModelModal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.models.services.ModelService',
    'mainApp.core.utilities.NavUtility'
])
.service('DeactivateModelModal', function ($compile, $templateCache, $rootScope, $http, ResourceUtility, ModelService) {
    var self = this;
    this.show = function (modelId) {
        $http.get('app/models/views/DeactivateModelConfirmView.html', { cache: $templateCache }).success(function (html) {

            var scope = $rootScope.$new();
            scope.modelId = modelId;

            var modalElement = $("#modalContainer");
            $compile(modalElement.html(html))(scope);
            $("#deactivateModelError").hide();

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
.controller('DeactivateModelController', function ($scope, $rootScope, $state, ResourceUtility, NavUtility, ModelService) {
    $scope.ResourceUtility = ResourceUtility;

    $scope.deactivateModelClick = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        updateAsInactiveModel($scope.modelId);
    };

    function updateAsInactiveModel(modelId) {
        $("#deactivateModelError").hide();
        ModelService.updateAsInactiveModel(modelId).then(function(result) {
            if (result != null && result.success === true) {
                $state.go('home.models', {}, { reload: true } );
            } else {
                console.log("errors");
            }
        });
    }

    $scope.cancelClick = function () {
        $("#modalContainer").modal('hide');
    };
});
