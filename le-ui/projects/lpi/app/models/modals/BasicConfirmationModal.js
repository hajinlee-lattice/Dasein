angular.module('mainApp.models.modals.BasicConfirmation', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility'
])
.service('BasicConfirmationModal', function($compile, $templateCache, $rootScope, $http, ResourceUtility) {
    var self = this;
    this.show = function(title, text, confirmLabel, cancelLabel, confirmationCb, cancelCb) {
        $http.get('app/models/views/BasicConfirmationView.html', { cache: $templateCache }).success(function(html) {

            var scope = $rootScope.$new();
            scope.title = title;
            scope.text = text;
            scope.confirmLabel = confirmLabel || ResourceUtility.getString('BUTTON_OK_LABEL');
            scope.cancelLabel = cancelLabel || ResourceUtility.getString('BUTTON_CANCEL_LABEL');
            scope.confirmationCb = confirmationCb || Function.prototype;
            scope.cancelCb = cancelCb || Function.prototype;

            var modalElement = $("#modalContainer");
            $compile(modalElement.html(html))(scope);

            var options = {
                backdrop: "static"
            };
            modalElement.modal(options);
            modalElement.modal('show');

            // Remove the created HTML from the DOM
            modalElement.on('hidden.bs.modal', function(evt) {
                modalElement.empty();
            });
        });
    };
})
.controller('BasicConfirmationController', function($scope, ResourceUtility) {
    $scope.ResourceUtility = ResourceUtility;

    $scope.confirmClick = function($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        $scope.confirmationCb();
        $("#modalContainer").modal('hide');
    };

    $scope.cancelClick = function() {
        $scope.cancelCb();
        $("#modalContainer").modal('hide');
    };
});
