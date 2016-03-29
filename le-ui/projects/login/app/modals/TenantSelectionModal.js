angular.module('mainApp.login.modals.TenantSelectionModal', [
    'mainApp.appCommon.utilities.UnderscoreUtility'
])
.service('TenantSelectionModal', function ($compile, $templateCache, $http, $rootScope, _) {
    this.show = function (tenantList, successCallback) {
        $http.get('app/views/TenantSelectionView.html', {
            cache: $templateCache
        }).success(function (html) {
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

            setTimeout(function() {
                $('input.tenant-selection-search').focus();
            },800);
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
    $scope.startsWith = function (actual, expected) {
        var lowerStr = (actual + "").toLowerCase();
        return lowerStr.indexOf(expected.toLowerCase()) === 0;
    };
});