angular.module('mainApp.setup.modals.UpdateFieldsModal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.setup.services.MetadataService',
    'mainApp.appCommon.utilities.StringUtility'
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
.controller('UpdateFieldsController', function ($scope, $rootScope, $state, ResourceUtility, StringUtility, MetadataService) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.updateClicked = false;
    $scope.saveInProgress = false;
    $scope.cloneError = false;

    $scope.updateFieldsClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        var modelName = StringUtility.SubstituteAllSpecialCharsWithDashes($scope.modelDisplayName);

        if ($scope.updateClicked) { return; }
        $scope.cloneError = false;
        $scope.updateClicked = true;

        $scope.saveInProgress = true;

        MetadataService.UpdateAndCloneFields(modelName, $scope.modelDisplayName, $scope.modelSummaryId, $scope.editedData).then(function(result){
            if (result.Success) {
                $("#modalContainer").modal('hide');

                $state.go('home.jobs.status', { 'jobCreationSuccess': true });
            } else {
                if (result.ResultErrors != null) {
                    $scope.updateFieldsErrorMessage = result.ResultErrors;
                } else {
                    $scope.updateFieldsErrorMessage = ResourceUtility.getString('UPDATE_FIELDS_ERROR_MESSAGE');
                }
                $scope.saveInProgress = false;
                $scope.cloneError = true;
                $scope.updateClicked = false;
                $("#updateFieldsError").fadeIn();
            }
        });
    };

    $scope.cancelClicked = function () {
        $scope.isCancelClicked = true;
        $("#modalContainer").modal('hide');
    };
});
