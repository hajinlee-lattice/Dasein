angular.module('mainApp.models.modals.RemodelingModal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility'
])
.service('RemodelingModal', function($compile, $templateCache, $rootScope, $http, ResourceUtility) {
    var self = this;
    this.show = function() {
        $http.get('app/models/views/RemodelingView.html', { cache: $templateCache }).success(function(html) {

            var scope = $rootScope.$new();

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

    this.hide = function() {
        $("#modalContainer").modal('hide');
    };
})
.controller('RemodelingController', function($scope, ResourceUtility) {
    $scope.ResourceUtility = ResourceUtility;
});
