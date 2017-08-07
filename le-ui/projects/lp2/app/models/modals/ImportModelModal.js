angular.module('mainApp.models.modals.ImportModelModal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.models.services.ModelService',
    'mainApp.core.utilities.NavUtility'
])
.directive('jsonUploader', ['$parse', function ($parse) {
    return {
        restrict: 'A',
        link: function(scope, element, attrs) {
            var model = $parse(attrs.jsonUploader);
            var modelSetter = model.assign;

            element.bind('change', function(){
                scope.$apply(function(){
                    modelSetter(scope, element[0].files[0]);
                });
            });
        }
    };
}])
.controller('jsonUploaderCtrl', ['$scope', '$rootScope', 'ModelService', 'ResourceUtility', function($scope, $rootScope, ModelService, ResourceUtility){
    $scope.showImportError = false;
    $scope.importErrorMsg = "";
    $scope.importing = false;
    $scope.showImportSuccess = false;
    $scope.ResourceUtility = ResourceUtility;

    // define reader
    var reader = new FileReader();

    // A handler for the load event (just defining it, not executing it right now)
    reader.onload = function() {
        $scope.$apply(function() {
            ModelService.uploadRawModelJSON(reader.result).then(function(result){
                if (result.Success) {
                    $scope.showImportSuccess = true;
                } else {
                    $scope.showImportError = true;
                    $scope.importErrorMsg = ResourceUtility.getString('VALIDATION_ERROR_GENERAL');
                    console.error(result.ResultErrors);
                }
                $scope.importing = false;
            });
        });
    };

    $scope.uploadFile = function(){
        $scope.showImportError = false;
        $scope.importErrorMsg = "";
        $scope.importing = true;
        reader.readAsText($scope.jsonFile);
    };


    $scope.okClicked = function(){
        $("#modalContainer").modal('hide');
        $rootScope.$broadcast("ModelCreationHistoryNavEvent");
    };

}])
.service('ImportModelModal', function ($compile, $rootScope, $http, NavUtility) {
    this.show = function () {
        $http.get('./app/models/views/ImportModelModalView.html').success(function (html) {

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
                $rootScope.$broadcast(NavUtility.MODEL_CREATION_HISTORY_NAV_EVENT);
            });
        });
    };
})
.controller('ImportModelModalController', function ($scope, $rootScope, ResourceUtility, NavUtility, ModelService) {
    $scope.ResourceUtility = ResourceUtility;
});
