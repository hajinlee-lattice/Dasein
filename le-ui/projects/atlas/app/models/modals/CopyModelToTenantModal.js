angular.module('mainApp.models.modals.CopyModelToTenantModal', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.models.services.ModelService',
    'mainApp.core.utilities.NavUtility'
])
.service('CopyModelToTenantModal', function ($compile, $templateCache, $rootScope, $http, NavUtility) {
    this.show = function (model) {
        $http.get('app/models/views/CopyModelToTenantModalView.html', { cache: $templateCache }).success(function (html) {

            var scope = $rootScope.$new();

            scope.model = model;

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
.controller('CopyModelToTenantModalController', function ($scope, $rootScope, $stateParams, 
    ResourceUtility, NavUtility, ModelService, CopyModelToTenantModal, BrowserStorageUtility) {
    var vm = this;
    //angular.extend(vm, {});
    $scope.vm = vm;

    vm.ResourceUtility = ResourceUtility;
    vm.asTenantName = $stateParams.tenantName || BrowserStorageUtility.getClientSession().Tenant.DisplayName;
    vm.current_tenant = {};
    vm.current_model = $scope.model;
    vm.copying = false;
    vm.modal_state = {
        choosing: true,
        copying: false,
        copied: false,
        error: false
    }
    
    var _tenants = $.jStorage.get('GriotLoginDocument').Tenants || {};
    vm.tenants = _tenants.filter(function(o) { 
        return o.DisplayName !== vm.asTenantName; 
    });

    vm.modal_change_state = function(key){
        vm.copying = false;
        _.each(vm.modal_state, function(_value,_key){
            vm.modal_state[_key] = false;
        });
        vm.modal_state[key] = true;
    }

    vm.selectTenant = function($event, tenant){
        var target = angular.element($event.currentTarget);
        deselectAll(target);
        target.addClass('selected');
        vm.current_tenant = tenant;
        vm.tenant_selected = true;
    }

    var deselectAll = function(target) {
        var target_type = target[0].tagName,
            targets = target.parent().find(target_type);
        vm.tenant_selected = false;
        vm.current_tenant = {};
        targets.removeClass('selected');
    }

    vm.copyingModel = function() {
        if(vm.current_model && vm.current_tenant) {
            vm.modal_change_state('copying');
            var modelName = vm.current_model.Name,
                tenantId = vm.current_tenant.Identifier;
        }
    }

    vm.copyModel = function() {
        if(vm.current_model && vm.current_tenant) {
            var modelName = vm.current_model.Id,
                tenantId = vm.current_tenant.Identifier;
            vm.copying = true;
            ModelService.CopyModel(modelName, tenantId).then(function(result){
                if (result != null && result.success === true) {
                    vm.modal_change_state('copied');
                } else {
                    vm.modal_change_state('error');
                    vm.copying_error = "There was a problem copying your model. This wasn't your fault. Please contact Lattice if this problem persists.";
                }
            });
        }
    }
});