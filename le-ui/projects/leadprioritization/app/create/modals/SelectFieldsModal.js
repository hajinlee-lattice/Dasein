angular.module('mainApp.setup.modals.SelectFieldsModal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.core.utilities.NavUtility',
    'mainApp.create.csvImport'
])
.service('SelectFieldsModal', function($compile, $templateCache, $rootScope, $http, ResourceUtility) {
    this.show = function(schema, fieldMapping) {

        $http.get('app/create/views/SelectFieldsView.html', { cache: $templateCache }).success(function (html) {
            var scope = $rootScope.$new();
            scope.schema = schema;
            scope.fieldName = fieldMapping.userField;

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
.controller('SelectFieldsController', function($scope, $rootScope, $state, ResourceUtility, NavUtility, csvImportService) {
    $scope.ResourceUtility = ResourceUtility;

    $scope.latticeSchemaFields;
    csvImportService.GetSchemaToLatticeFields().then(function(result) {
        $scope.latticeSchemaFields = result[$scope.schema];
    });

    $scope.mapLatticeField = function(latticeSchemaField) {
        $rootScope.$broadcast(NavUtility.MAP_LATTICE_SCHEMA_FIELD_EVENT, { 'userFieldName': $scope.fieldName, 'latticeSchemaField': latticeSchemaField} );

        $("#modalContainer").modal('hide');
    }

    $scope.getRequiredType = function(latticeSchemaField) {
        if (latticeSchemaField.requiredType == "Required") {
            return latticeSchemaField.requiredType;
        } else if (latticeSchemaField.requiredType == "RequiredIfOtherFieldIsEmpty" && latticeSchemaField.requiredIfNoField != null) {
            return "Required if no " + latticeSchemaField.requiredIfNoField;
        }

        return;
    };

    $scope.fieldClicked = function(field) {

    };

    $scope.cancelClicked = function () {
        $scope.isCancelClicked = true;
        $("#modalContainer").modal('hide');
    };
});
