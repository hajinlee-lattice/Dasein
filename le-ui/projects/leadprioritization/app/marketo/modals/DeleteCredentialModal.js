angular.module('mainApp.marketo.modals.DeleteCredentialModal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'lp.marketo'
])
.service('DeleteCredentialModal', function ($compile, $templateCache, $rootScope, $http, ResourceUtility, MarketoService) {
    var self = this;
    this.show = function (credentialId) {
        $http.get('app/marketo/views/DeleteCredentialModalView.html', { cache: $templateCache }).success(function (html) {

            var scope = $rootScope.$new();
            scope.credentialId = credentialId;

            var modalElement = $("#modalContainer");
            $compile(modalElement.html(html))(scope);
            $("#deleteCredentialError").hide();

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
.controller('DeleteCredentialController', function ($scope, $rootScope, $state, ResourceUtility, MarketoService) {
    $scope.ResourceUtility = ResourceUtility;

    $scope.deleteCredentialClick = function () {
        $("#deleteModelError").hide();
        MarketoService.DeleteMarketoCredential($scope.credentialId).then(function(result) {
            if (result != null && result.success === true) {
                $("#modalContainer").modal('hide');
                $state.go('home.marketosettings.apikey', {}, { reload: true } );
            } else {
                $scope.deleteCredentialErrorMessage = result.ResultErrors;
                $("#deleteModelError").fadeIn();
            }
        });
    };

    $scope.cancelClick = function () {
        $("#modalContainer").modal('hide');
    };
});
