angular.module('mainApp.models.modals.CopyModelModal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.models.services.ModelService',
    'mainApp.core.utilities.NavUtility'
])
.service('CopyModelModal', function ($compile, $templateCache, $rootScope, $http, NavUtility) {
    this.show = function () {
        $http.get('app/models/views/CopyModelModalView.html', { cache: $templateCache }).success(function (html) {

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
            });
        });
    };
})
.controller('CopyModelModalController', function ($scope, $rootScope, ResourceUtility, NavUtility, ModelService) {
    var vm = this;
    //angular.extend(vm, {});
    $scope.vm = vm;

    vm.ResourceUtility = ResourceUtility;

    vm.tenants = $.jStorage.get('GriotLoginDocument').Tenants || {};
    vm.current_tenant = {};
    vm.models = [];
    vm.current_model = {};

    vm.selectTenant = function($event, tenant){
        var target = angular.element($event.currentTarget);

        deselectAll(target);

        target.addClass('selected');

        vm.current_tenant = tenant;
        vm.gettingModels = true;
        vm.models = [];
        ModelService.GetAllModels(false, tenant.Identifier).then(function(result) {
            $scope.loading = false;
            if (result != null && result.success === true) {
                vm.models = result.resultObj;
            } else if (result.resultErrors === "NO TENANT FOUND") {
                vm.showNoModels = true;
            }
        });
    }

    var deselectAll = function(target) {
        var target_type = target[0].tagName,
            targets = target.parent().find(target_type);
        targets.removeClass('selected');
    }

    vm.selectModel = function($event, model){
        var target = angular.element($event.currentTarget);

        deselectAll(target);

        target.addClass('selected');

        vm.current_model = model;
    }
    vm.copyModel = function() {
        if(vm.current_model && vm.current_tenant) {
            var modelName = vm.current_model.Name;
            var tenantId = vm.current_tenant.Identifier;
            //move to ModelService.js
            $http.get('/pls/models/copymodel/' + modelName, {params: {targetTenantId: tenantId}}).success(function (data) {
                console.log(data);
            });
        }
    }
});