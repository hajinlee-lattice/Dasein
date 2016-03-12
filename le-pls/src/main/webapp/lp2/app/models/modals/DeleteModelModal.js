angular.module('mainApp.models.modals.DeleteModelModal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.models.services.ModelService',
    'mainApp.core.utilities.NavUtility'
])
.service('DeleteModelModal', function ($compile, $rootScope, $http, ResourceUtility, ModelService) {
    var self = this;
    this.show = function (modelId) {
        $http.get('./app/models/views/DeleteModelConfirmView.html').success(function (html) {
            
            var scope = $rootScope.$new();
            scope.modelId = modelId;
            
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
.controller('DeleteModelController', function ($scope, $rootScope, ResourceUtility, NavUtility, ModelService) {
    $scope.ResourceUtility = ResourceUtility;
    
    $scope.deleteModelClick = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
    
        updateAsDeletedModel($scope.modelId);            
    };
    
    function updateAsDeletedModel(modelId) {
        $("#deleteModelError").hide();
        ModelService.updateAsDeletedModel(modelId).then(function(result) {
            if (result != null && result.success === true) {
                $("#modalContainer").modal('hide');
                $rootScope.$broadcast(NavUtility.MODEL_LIST_NAV_EVENT);                                  
            } else {
                $scope.deleteModelErrorMessage = result.ResultErrors;
                $("#deleteModelError").fadeIn();
            }
        });
    }
    
    $scope.cancelClick = function () {
        $("#modalContainer").modal('hide');
    };
});
