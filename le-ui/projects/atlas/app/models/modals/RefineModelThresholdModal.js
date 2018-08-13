angular.module('mainApp.models.modals.RefineModelThresholdModal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.models.services.ModelService'
])
.service('RefineModelThresholdModal', function ($compile, $templateCache, $rootScope, $http, ResourceUtility, StringUtility, ModelService) {
    var self = this;
    this.show = function (totalRows, successEvents, conversionRate) {
        $http.get('app/models/views/RefineModelThresholdModal.html', { cache: $templateCache }).success(function (html) {
            var scope = $rootScope.$new();
            scope.ResourceUtility = ResourceUtility;
            scope.totalRows = totalRows;
            scope.totalRowsDisplay = StringUtility.AddCommas(totalRows);
            scope.successEvents = successEvents;
            scope.successEventsDisplay = StringUtility.AddCommas(successEvents);
            scope.conversionRate = conversionRate;

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
.controller('RefineModelThresholdController', function($scope, $rootScope, ResourceUtility) {
    $scope.createModelClicked = function() {
        $rootScope.$broadcast('ShowCreateModelPopup');
    };

    $scope.cancelClicked = function() {
        $("#modalContainer").modal('hide');
    };
});
