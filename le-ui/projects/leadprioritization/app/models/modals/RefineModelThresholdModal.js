angular.module('mainApp.models.modals.RefineModelThresholdModal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.models.services.ModelService'
])
.service('RefineModelThresholdModal', function ($compile, $templateCache, $rootScope, $http, ResourceUtility, ModelService) {
    var self = this;
    this.show = function () {
        $http.get('app/models/views/RefineModelThresholdModal.html', { cache: $templateCache }).success(function (html) {

            var scope = $rootScope.$new();

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
});
