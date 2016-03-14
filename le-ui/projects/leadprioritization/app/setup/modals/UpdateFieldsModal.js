angular.module('mainApp.setup.modals.UpdateFieldsModal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.setup.services.MetadataService'
])
.service('UpdateFieldsModal', function ($compile, $templateCache, $rootScope, $http, ResourceUtility) {
    var self = this;
    this.show = function (modelSummaryId, editedData) {
        $http.get('app/setup/views/UpdateFieldsView.html', { cache: $templateCache }).success(function (html) {

            var scope = $rootScope.$new();
            scope.modelSummaryId = modelSummaryId;
            scope.editedData = editedData;

            var modalElement = $("#modalContainer");
            $compile(modalElement.html(html))(scope);
            $("#deleteModelError").hide();

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
.controller('UpdateFieldsController', function ($scope, $rootScope, $state, ResourceUtility, MetadataService) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.modelNameInvalid = false;

    $scope.updateFieldsClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        if ($scope.modelName == null) {
            $scope.modelNameInvalid = true;
            return;
        }

        MetadataService.UpdateAndCloneFields($scope.modelName, $scope.modelSummaryId, $scope.editedData).then(function(result){
            if (result.Success) {
                $("#modalContainer").modal('hide');

                $state.go('jobs');
            } else {
                if (result.ResultErrors != null) {
                    $scope.updateFieldsErrorMessage = result.ResultErrors;
                } else {
                    $scope.updateFieldsErrorMessage = ResourceUtility.getString('UPDATE_FIELDS_ERROR_MESSAGE');
                }
                $("#updateFieldsError").fadeIn();
            }
        });
    };

    $scope.cancelClicked = function () {
        $("#modalContainer").modal('hide');
    };
});
