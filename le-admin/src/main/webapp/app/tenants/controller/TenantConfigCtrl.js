var app = angular.module("app.tenants.controller.TenantConfigCtrl", [
    'app.tenants.service.TenantService',
    'app.tenants.directive.CamilleConfigDirective',
    'le.common.util.UnderscoreUtility',
    "app.tenants.util.TenantUtility",
    'ui.bootstrap',
    'ui.router',
    'ngSanitize'
]);

app.controller('TenantConfigCtrl', function($scope, $state, $stateParams, $modal, $interval, _, TenantService, TenantUtility) {
    $scope.new = false;
    $scope.readonly = true;
    $scope.listenState = true;

    if ($stateParams.new) $scope.new = $stateParams.new === "true";
    if ($stateParams.readonly) $scope.readonly = $stateParams.readonly === "true";
    if ($stateParams.listenState) $scope.listenState = $stateParams.listenState === "true";

    $scope.tenantId = $stateParams.tenantId;
    $scope.contractId = $stateParams.contractId || $stateParams.tenantId;
    $scope.space = $stateParams.space || "Production";
    $scope.product = $stateParams.product;

    $scope.loading = true;
    $scope.services = TenantService.registeredServices;

    $scope.accordion = _.map($scope.services, function(){
        return { open: false, disabled: false };
    });

    $scope.components = [];
    $scope.isValid = {valid: true};

    if ($scope.new) {
        _.each($scope.services, function(service){
            TenantService.GetServiceDefaultConfig(service).then(
                function(result){
                    var component = result.resultObj;
                    $scope.components.push(component);
                    if ($scope.components.length == $scope.services.length) {
                        $scope.loading = false;
                    }
                }
            );
        });
    } else {
        _.each($scope.services, function(service){
            TenantService.GetTenantServiceConfig($scope.tenantId, $scope.contractId, service).then(
                function(result){
                    var component = result.resultObj;
                    $scope.components.push(component);
                    if ($scope.components.length == $scope.services.length) {
                        $scope.loading = false;
                    }
                }
            );
        });
    }

    function updateServiceStatus() {
        _.each($scope.components, function(component){
            TenantService.GetTenantServiceStatus($scope.tenantId, $scope.contractId, component.Component).then(
                function(result){
                    component.State = result.resultObj;
                    //console.debug("The status of " + component.Component + " is updated to " + component.State.state);
                }
            );
        });
    }

    if ($scope.listenState) {
        var statusUpdater = $interval(function(){
            if ($state.current.name !== "TENANT.CONFIG") {
                $interval.cancel(statusUpdater);
            }
            if (!$scope.loading) {
                updateServiceStatus();
            }
        }, 5000);
    }

    $scope.onSaveClick = function(){
        $scope.tenantRegisration = TenantUtility.constructTenantRegistration($scope.components, $scope.tenantId, $scope.contractId, $scope.space);

        $modal.open({
            template: '<div class="modal-header">' +
            '<h3 class="modal-title">Data to be saved.</h3></div>' +
            '<div class="modal-body">' +
            '<pre ng-hide="showErrorMsg">{{ data | json }}</pre>' +
            '<p class="text-danger" ng-show="showErrorMsg">{{ errorMsg }}</p>' +
            '</div>' +
            '<div class="modal-footer">' +
            '<button class="btn btn-primary" ng-hide="showErrorMsg" ng-click="submit()">OK</button>' +
            '<button class="btn btn-default" ng-click="cancel()">CANCEL</button>' +
            '</div>',
            controller: function($scope, $modalInstance, data, contractId, tenantId, TenantService){
                $scope.data = data;
                $scope.tenantId = tenantId;
                $scope.contractId = contractId;
                $scope.showErrorMsg = false;

                $scope.submit = function() {
                    $scope.showErrorMsg = false;
                    TenantService.CreateTenant($scope.tenantId, $scope.contractId, $scope.data).then(
                        function(result) {
                            if (result.success) {
                                $modalInstance.dismiss();
                                $state.go('TENANT.LIST');
                            } else {
                                $scope.errorMsg = "Adding tenant failed.";
                                $scope.showErrorMsg = true;
                            }
                        }
                    );
                };

                $scope.cancel = function () {
                    $modalInstance.dismiss('cancel');
                };
            },
            resolve: {
                data: function () { return $scope.tenantRegisration; },
                tenantId: function () { return $scope.tenantId; },
                contractId: function () { return $scope.contractId; }
            }
        });
    };


    $scope.onDeleteClick = function(){
        $modal.open({
            template: '<div class="modal-header">' +
            '<h3 class="modal-title">Delete tenant</h3></div>' +
            '<div class="modal-body">Are you sure you want to delete the tenant {{ tenantId }}.</div>' +
            '<div class="modal-footer">' +
            '<button class="btn btn-primary" ng-click="ok()">YES</button>' +
            '<button class="btn btn-default" ng-click="cancel()">NO</button>' +
            '</div>',
            controller: function($scope, $state, $modalInstance, tenantId, contractId, TenantService){
                $scope.tenantId = tenantId;
                $scope.contractId = contractId;

                $scope.ok = function() {
                    TenantService.DeleteTenant(tenantId, contractId).then(function(result){
                        if (result.success) {
                            $modalInstance.dismiss();
                            $state.go('TENANT.LIST');
                        } else {
                            //handle error
                        }
                    });
                };

                $scope.cancel = function () {
                    $modalInstance.dismiss();
                };
            },
            resolve: {
                tenantId: function () { return $scope.tenantId; },
                contractId: function () { return $scope.contractId; }
            }
        });
    };
});


