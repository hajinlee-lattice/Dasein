var app = angular.module("app.tenants.controller.TenantConfigCtrl", [
    'app.tenants.service.TenantService',
    'app.services.service.ServiceService',
    'app.tenants.directive.CamilleConfigDirective',
    'le.common.util.UnderscoreUtility',
    "app.tenants.util.TenantUtility",
    'ui.bootstrap',
    'ui.router',
    'ngSanitize'
]);

app.controller('TenantConfigCtrl', function($scope, $state, $stateParams, $modal, $interval, _, TenantService, TenantUtility, ServiceService) {
    $scope.new = false;
    $scope.readonly = true;
    $scope.listenState = true;

    if ($stateParams.new) $scope.new = $stateParams.new === "true";
    if ($stateParams.readonly) $scope.readonly = $stateParams.readonly === "true";
    if ($stateParams.listenState) $scope.listenState = $stateParams.listenState === "true";

    $scope.tenantId = $stateParams.tenantId;
    $scope.contractId = $stateParams.contractId || $stateParams.tenantId;
    $scope.product = $stateParams.product;

    $scope.loading = true;
    $scope.services = [];

    $scope.accordion = _.map($scope.services, function(){
        return { open: false, disabled: false };
    });

    $scope.components = [];
    $scope.isValid = {valid: true};

    if ($scope.new) {
        $scope.spaceInfo = {
            properties: {
                displayName: "LPA_" + $scope.tenantId,
                description: "A LPA solution for " + $scope.tenantId + " in " + $scope.contractId,
                product: $scope.product
            },
            featureFlags: ""
        };

        ServiceService.GetRegisteredServices().then( function(result) {
            if (result.success) {
                $scope.services = result.resultObj;
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
            }
        });

    } else {
        TenantService.GetTenantInfo($scope.tenantId, $scope.contractId).then(function(result1){
            if (result1.success) {
                $scope.spaceInfo = result1.resultObj.spaceInfoList[0];
                $scope.contractInfo = result1.resultObj.contractInfo;
                $scope.tenantInfo = result1.resultObj;
                ServiceService.GetRegisteredServices().then( function(result2) {
                    if (result2.success) {
                        $scope.services = result2.resultObj;
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
                });
            }
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
        var infos = {CustomerSpaceInfo: $scope.spaceInfo};

        $scope.tenantRegisration =
            TenantUtility.constructTenantRegistration($scope.components, $scope.tenantId, $scope.contractId, infos);

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
});


