angular.module('mainApp.setup.modals.FieldMappingSettingsModal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.NavUtility',
])
.service('FieldMappingSettingsModal', function ($compile, $templateCache, $rootScope, $http, ResourceUtility) {
    var self = this;
    this.show = function(oneLeadPerDomain, includePersonalEmailDomains, useLatticeAttributes) {
        $http.get('app/setup/views/FieldMappingSettingsView.html', { cache: $templateCache }).success(function (html) {
            var scope = $rootScope.$new();
            scope.oneLeadPerDomain = oneLeadPerDomain;
            scope.includePersonalEmailDomains = includePersonalEmailDomains;
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

    this.showForModelCreation = function (oneLeadPerDomain, includePersonalEmailDomains, useLatticeAttributes) {
        $http.get('app/setup/views/FieldMappingSettingsView.html', { cache: $templateCache }).success(function (html) {
            var scope = $rootScope.$new();
            scope.oneLeadPerDomain = oneLeadPerDomain;
            scope.includePersonalEmailDomains = includePersonalEmailDomains;
            scope.useLatticeAttributes = useLatticeAttributes;
            scope.modelCreation = true;

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
.controller('FieldMappingSettingsController', function ($scope, $rootScope, $state, ResourceUtility, NavUtility, ImportStore) {
    $scope.ResourceUtility = ResourceUtility;

    $scope.saveSettingsClicked = function() {
        if (!$scope.modelCreation) {
            $rootScope.$broadcast(NavUtility.MANAGE_FIELDS_ADVANCED_SETTINGS_EVENT, $scope.oneLeadPerDomain, $scope.includePersonalEmailDomains, $scope.useLatticeAttributes);
            $("#modalContainer").modal('hide');
        } else {
            ImportStore.SetAdvancedSettings('oneLeadPerDomain', $scope.oneLeadPerDomain);
            ImportStore.SetAdvancedSettings('includePersonalEmailDomains', $scope.includePersonalEmailDomains);
            ImportStore.SetAdvancedSettings('useLatticeAttributes', $scope.useLatticeAttributes);
            $("#modalContainer").modal('hide');
        }
    };

    $scope.cancelClicked = function() {
        $("#modalContainer").modal('hide');
    };
});
