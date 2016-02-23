angular.module('mainApp.setup.controllers.SaveAttributesModel', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.setup.services.LeadEnrichmentService'
])

.service('SaveAttributesModel', function ($compile, $http) {

    this.show = function ($parentScope, attributes) {
        $http.get('./app/setup/views/SaveAttributesConfirmView.html').success(function (html) {

            var scope = $parentScope.$new();
            scope.attributes = attributes;

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

.controller('SaveAttributesController', function ($scope, _, ResourceUtility, LeadEnrichmentService) {
    $scope.ResourceUtility = ResourceUtility;

    $scope.yesClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }

        $scope.verifyInProgress = true;
        $scope.errorMessage = null;
        $scope.tables = null;
        LeadEnrichmentService.VerifyAttributes($scope.attributes).then(function (data) {
            if (data.Success) {
                var resultTables = data.ResultObj;
                if (resultTables.length === 0) {
                    $scope.$parent.saveAttributes($scope.attributes);
                    $("#modalContainer").modal('hide');
                } else {
                    var tables = [resultTables.length];
                    for (var i = 0; i < resultTables.length; i++) {
                        var resultTable = resultTables[i];
                        var invalidFields = [];
                        for (var j = 0; j < resultTable.invalidFields.length; j++) {
                            var attr = _.findWhere($scope.$parent.selectedAttributes, { FieldName: resultTable.invalidFields[j] });
                            if (attr != null) {
                                invalidFields.push('Lattice_' + attr.FieldNameInTarget);
                            }
                        }
                        tables[i] = { tableName: resultTable.tableName, invalidFields: invalidFields };
                    }
                    $scope.tables = tables;
                    $scope.verifyInProgress = false;
                }
            } else {
                $scope.errorMessage = data.ResultErrors;
                $scope.verifyInProgress = false;
            }
        });
    };

    $scope.noClicked = function () {
        $("#modalContainer").modal('hide');
    };
});