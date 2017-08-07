angular.module('mainApp.login.modals.TenantSelectionModal', [
    'mainApp.appCommon.utilities.UnderscoreUtility'
])
.service('TenantSelectionModal', function ($compile, $http, $rootScope, _) {
    this.show = function (tenantList, successCallback) {
        $http.get('./app/login/views/TenantSelectionView.html').success(function (html) {
            
            var scope = $rootScope.$new();
            scope.tenantList = _.sortBy(tenantList, 'Indentifier');
            scope.successCallback = successCallback;
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
.controller('TenantSelectionController', function ($scope, ResourceUtility) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.handleTenantSelected = function () {
        if (this.select_value != null) {
            var tenant = _.find($scope.tenantList, {'Identifier': this.select_value});
            $scope.successCallback(tenant);
            $("#modalContainer").modal('hide');
        }
    };
});
