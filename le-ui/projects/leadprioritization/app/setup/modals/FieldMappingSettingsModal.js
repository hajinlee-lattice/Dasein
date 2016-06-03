angular.module('mainApp.setup.modals.FieldMappingSettingsModal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.NavUtility'
])
.service('FieldMappingSettingsModal', function ($compile, $templateCache, $rootScope, $http, ResourceUtility) {
    var self = this;
    this.show = function (oneLeadPerDomain, useLatticeAttributes) {
        $http.get('app/setup/views/FieldMappingSettingsView.html', { cache: $templateCache }).success(function (html) {
            var scope = $rootScope.$new();
            scope.oneLeadPerDomain = oneLeadPerDomain;
            scope.useLatticeAttributes = useLatticeAttributes;

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
.controller('FieldMappingSettingsController', function ($scope, $rootScope, $state, ResourceUtility, NavUtility) {
    $scope.ResourceUtility = ResourceUtility;

    $scope.saveSettingsClicked = function() {
        $rootScope.$broadcast(NavUtility.MANAGE_FIELDS_ADVANCED_SETTINGS_EVENT, $scope.oneLeadPerDomain, $scope.useLatticeAttributes);
        $("#modalContainer").modal('hide');
    };

    $scope.cancelClicked = function() {
        $("#modalContainer").modal('hide');
    };
});
