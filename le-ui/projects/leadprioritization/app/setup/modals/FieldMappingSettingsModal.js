angular.module('mainApp.setup.modals.FieldMappingSettingsModal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.NavUtility',
])
.service('FieldMappingSettingsModal', function ($compile, $templateCache, $rootScope, $http, ResourceUtility) {
    var self = this;
    this.show = function(oneLeadPerDomain, includePersonalEmailDomains, useLatticeAttributes, enableTransformations, sourceType) {
        $http.get('app/setup/views/FieldMappingSettingsView.html', { cache: $templateCache }).success(function (html) {
            var scope = $rootScope.$new();
            scope.oneLeadPerDomain = oneLeadPerDomain;
            scope.includePersonalEmailDomains = includePersonalEmailDomains;
            scope.useLatticeAttributes = useLatticeAttributes;
            scope.enableTransformations = enableTransformations;
            scope.sourceType = sourceType;

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

    this.showForModelCreation = function (oneLeadPerDomain, includePersonalEmailDomains, useLatticeAttributes, enableTransformations, sourceType) {
        $http.get('app/setup/views/FieldMappingSettingsView.html', { cache: $templateCache }).success(function (html) {
            var scope = $rootScope.$new();
            scope.oneLeadPerDomain = oneLeadPerDomain;
            scope.includePersonalEmailDomains = includePersonalEmailDomains;
            scope.useLatticeAttributes = useLatticeAttributes;
            scope.enableTransformations = enableTransformations;
            scope.sourceType = sourceType;
            scope.modelCreation = true;

            var infoAccount = "Excludes all the duplicate lead records that belong to the same account within a country and keeps one which has a positive event. Maintains a ratio of positive to negative events desired for better model training",
                infoLead = "Excludes all the duplicate records that belong to the same account within a country and keeps one which has a positive event. Maintains a ratio of positive to negative events desired for better model training";
            scope.sourceTypeInfoTemplate = "<div class='row'><div class='twelve columns'><p>" + (sourceType != 'SalesforceAccount' ? infoAccount : infoLead ) + "</p></div></div>";

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
            $rootScope.$broadcast(NavUtility.MANAGE_FIELDS_ADVANCED_SETTINGS_EVENT, $scope.oneLeadPerDomain, $scope.includePersonalEmailDomains, $scope.useLatticeAttributes, $scope.enableTransformations);
            $("#modalContainer").modal('hide');
        } else {
            ImportStore.SetAdvancedSettings('oneLeadPerDomain', $scope.oneLeadPerDomain);
            ImportStore.SetAdvancedSettings('includePersonalEmailDomains', $scope.includePersonalEmailDomains);
            ImportStore.SetAdvancedSettings('useLatticeAttributes', $scope.useLatticeAttributes);
            ImportStore.SetAdvancedSettings('enableTransformations', $scope.enableTransformations);
            $("#modalContainer").modal('hide');
        }
    };

    $scope.cancelClicked = function() {
        $("#modalContainer").modal('hide');
    };
});
