var app = angular.module("app.tenants.controller.TenantConfigCtrl", [
    'app.tenants.service.TenantService',
    'app.services.service.ServiceService',
    'app.tenants.directive.CamilleConfigDirective',
    'app.tenants.directive.FeatureFlagDirective',
    'le.common.util.UnderscoreUtility',
    "app.tenants.util.TenantUtility",
    'ui.bootstrap',
    'ui.router',
    'ngSanitize'
]);

app.controller('TenantConfigCtrl', function($scope, $rootScope, $timeout, $state, $stateParams, $modal, $interval, _, TenantService, TenantUtility, ServiceService) {
    //==================================================
    // initialize flags
    //==================================================
    $scope.new = false;
    $scope.readonly = true;
    $scope.listenState = true;
    $scope.loading = true;

    //==================================================
    // parse state parameters flags
    //==================================================
    if ($stateParams.new) $scope.new = $stateParams.new === "true";
    if ($stateParams.readonly) $scope.readonly = $stateParams.readonly === "true";
    if ($stateParams.listenState) $scope.listenState = $stateParams.listenState === "true";

    //==================================================
    // setup customer space ==> 3 IDs
    //==================================================
    $scope.tenantId = $stateParams.tenantId;
    $scope.contractId = $stateParams.contractId || $stateParams.tenantId;
    $scope.spaceId = "Production";
    $scope.product = $stateParams.product;

    //==================================================
    // system-wise options
    //==================================================
    $scope.availableProducts = ["Lead Prioritization"];
    $scope.availableTopologies = ["Marketo"];
    $scope.availableDLAddresses = ["http://bodcdevvint207.dev.lattice.local:8081"];
    $scope.services = [];
    updateSpaceConfigurationOptions();

    //==================================================
    // initialization
    //==================================================
    $scope.accordion = _.map($scope.services, function(){
        return { open: false, disabled: false };
    });
    $scope.components = [];
    $scope.isValid = {valid: true};

    if ($scope.new) {
        constructNewPage();
    } else {
        constructViewPage($scope.tenantId, $scope.contractId);
    }

    if ($scope.listenState) {
        var statusUpdater = $interval(function(){
            if ($state.current.name !== "TENANT.CONFIG") {
                $interval.cancel(statusUpdater);
            }
            if (!$scope.loading) {
                updateServiceStatus();
            }
        }, 3000);
    }

    //==================================================
    // other definitions
    //==================================================
    $scope.onSaveClick = function(){
        var infos = {
            CustomerSpaceInfo: $scope.spaceInfo,
            TenantInfo: $scope.tenantInfo
        };

        $scope.tenantRegisration =
            TenantUtility.constructTenantRegistration($scope.components,
                $scope.tenantId, $scope.contractId, infos, $scope.spaceConfig, $scope.featureFlags);

        popInstallConfirmationModal();
    };


    $scope.onDeleteClick = function(){ popDeleteConfirmationModal(); };

    $scope.$on("CALC_DERIVED", function(evt, data) {
        var derivation = data.derivation;
        var derivedValue = TenantUtility.calcDerivation($scope.components, derivation, $scope);
        data.callback(derivedValue);
    });

    function constructNewPage() {
        $scope.spaceInfo = {
            properties: {
                displayName: "LPA_" + $scope.tenantId,
                description: "A LPA solution for " + $scope.tenantId + " in " + $scope.contractId,
                product: $scope.availableProducts[0],
                topology: $scope.availableTopologies[0]
            },
            featureFlags: ""
        };
        $scope.featureFlags = {
            Dante: false
        };
        $scope.tenantInfo = {
            properties: {
                displayName: "LPA " + $scope.tenantId,
                description: "A LPA tenant under the contract " + $scope.contractId
            }
        };

        TenantService.GetDefaultSpaceConfiguration().then( function(result) {
            if (result.success) {
                $scope.spaceConfig = result.resultObj;
            } else {
                console.log("Getting default space configuration error");
                $state.go("TENANT.LIST");
            }
        });

        ServiceService.GetRegisteredServices().then( function(result) {
            if (result.success) {
                $scope.services = result.resultObj;
                $scope.defaultConfigScaned = 0;
                _.each($scope.services, function(service){
                    ServiceService.GetServiceDefaultConfig(service).then(
                        function(result){
                            $scope.defaultConfigScaned += 1;
                            if (result.success) {
                                var component = result.resultObj;
                                $scope.components.push(component);
                            }
                            if ($scope.defaultConfigScaned == $scope.services.length) {
                                $scope.loading = false;
                                $timeout(function(){$rootScope.$broadcast("UPDATE_DERIVED");}, 500);
                            }
                        }
                    );
                });
            } else {
                console.log("Getting registered service error");
                $state.go("TENANT.LIST");
            }
        });
    }

    function constructViewPage(tenantId, contractId) {
        TenantService.GetTenantInfo(tenantId, contractId).then(function(result1){
            if (result1.success) {
                $scope.spaceInfo = result1.resultObj.CustomerSpaceInfo;
                $scope.contractInfo = result1.resultObj.ContractInfo;
                $scope.tenantInfo = result1.resultObj.TenantInfo;
                $scope.spaceConfig = result1.resultObj.SpaceConfiguration;

                try {
                    $scope.featureFlags = JSON.parse($scope.spaceInfo.featureFlags);
                } catch (err) {
                    $scope.featureFlags = "";
                }

                ServiceService.GetRegisteredServices().then( function(result2) {
                    if (result2.success) {
                        $scope.services = result2.resultObj;
                        $scope.componentsToScan = $scope.services.length;
                        _.each($scope.services, function(service){
                            if (!$scope.featureFlags.Dante && service === "Dante") {
                                $scope.componentsToScan--;
                                return;
                            }
                            TenantService.GetTenantServiceConfig($scope.tenantId, $scope.contractId, service).then(
                                function(result){
                                    $scope.componentsToScan--;
                                    var component;
                                    if (result.success) {
                                        component = result.resultObj;
                                        component = changeComponentToMessage(component);
                                    } else {
                                        component = {
                                            Component: service,
                                            Message: result.errorMsg
                                        };
                                    }
                                    $scope.components.push(component);
                                    if ($scope.componentsToScan <= 0) {
                                        if ($scope.listenState) updateServiceStatus();
                                        $scope.loading = false;
                                    }
                                }
                            );
                        });
                    } else {
                        console.log("Getting registered service error");
                        $state.go("TENANT.LIST");
                    }
                });
            }
        });
    }

    function popInstallConfirmationModal() {
        $modal.open({
            template: '<div class="modal-header">' +
            '<h3 class="modal-title">About to bootstrap a new tenant.</h3></div>' +
            '<div class="modal-body">' +
            '<p ng-hide="showErrorMsg">Are you sure to start bootstrapping tenant {{ tenantId }}?</p>' +
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
    }

    function popDeleteConfirmationModal() {
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
    }

    function updateServiceStatus() {
        var componentsToScan = $scope.components.length;
        _.each($scope.components, function (component) {
            TenantService.GetTenantServiceStatus($scope.tenantId, $scope.contractId, component.Component).then(
                function (result) {
                    componentsToScan--;
                    var newState = result.resultObj;
                    if (typeof(component.State) === "undefined" ||
                        newState.state !== component.State.state) {
                        component.State = newState;
                        TenantService.GetTenantServiceConfig(
                            $scope.tenantId, $scope.contractId, component.Component).then(
                            function (result) {
                                var newComponent = result.resultObj;
                                component.RootPath = newComponent.RootPath;
                                component.Nodes = newComponent.Nodes;
                                changeComponentToMessage(component);
                                $scope.$broadcast("UPDATE_DERIVED");
                            }
                        );
                    }
                }
            );
        });
    }

    function updateSpaceConfigurationOptions() {
        ServiceService.GetSpaceConfigOptions().then(function(result){
            if(result.success) {
                var options = result.resultObj;
                _.each(options.Nodes, function(node){
                    switch (node.Node.substring(1)) {
                        case "DL_Address":
                            $scope.availableDLAddresses = node.Options;
                            break;
                        case "Product":
                            $scope.availableProducts = node.Options;
                            break;
                        case "Topology":
                            $scope.availableTopologies = node.Options;
                            break;
                        case "Template_Path":
                            $scope.availableTemplatePath = node.Options;
                            break;
                    }
                });
            }
        });
    }

    function changeComponentToMessage(component) {
        if (component.hasOwnProperty("State") &&
            component.State.hasOwnProperty("errorMessage") &&
            component.State.errorMessage !== "") {
            component.Message = component.State.errorMessage;
        }
        return component;
    }

});


