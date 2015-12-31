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
    $scope.availableProductsNames = ["Lead Prioritization"];
    $scope.availableTopologies = ["Marketo"];
    $scope.availableDLAddresses = ["http://bodcdevvint207.dev.lattice.local:8081"];
    $scope.services = [];
    $scope.servicesWithProducts = [];
    $scope.featureFlagDefinitions = [];
    $scope.selectedFeatureFlags = [];
    $scope.selectedComponents = [];
    updateAvailableProducts();
    updateSpaceConfigurationOptions();
    getAvailableFeatureFlags();
    getAvailableComponents();


    //==================================================
    // initialization
    //==================================================
    $scope.accordion = _.map($scope.services, function(){
        return { open: false, disabled: false };
    });
    $scope.components = [];
    $scope.isValid = {valid: true};
    $scope.selectedProductNames=[];
    $scope.selectedProducts=[];


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
        }, 5000);
    }

    //==================================================
    // other definitions
    //==================================================
    $scope.onSaveClick = function(){
        $scope.spaceConfig.Products = $scope.selectedProductNames;
        var infos = {
            CustomerSpaceInfo: $scope.spaceInfo,
            TenantInfo: $scope.tenantInfo
        };

        $scope.featureFlags = _.object(_.map($scope.selectedFeatureFlags, function(x){return [x.DisplayName, x.Value];}));
        console.log("featureFlags are: " + $scope.featureFlags);

        $scope.tenantRegisration =
            TenantUtility.constructTenantRegistration($scope.selectedComponents,
                $scope.tenantId, $scope.contractId, infos, $scope.spaceConfig, $scope.featureFlags);

        popInstallConfirmationModal();
    };

    $scope.onDeleteClick = function(){ popDeleteConfirmationModal(); };

    $scope.$on("CALC_DERIVED", function(evt, data) {
        var derivation = data.derivation;
        var derivedValue = TenantUtility.calcDerivation($scope.components, derivation, $scope);
        data.callback(derivedValue);
    });
    
    $scope.toggleSelection = function toggleSelection(productName) {
        var idx = $scope.selectedProductNames.indexOf(productName);
        if (idx > -1) {
          $scope.selectedProductNames.splice(idx, 1);
          $scope.selectedProducts = _.reject($scope.selectedProducts, function(product) { return product.name === productName; });
        }
        else {
          $scope.selectedProductNames.push(productName);
          // add the selected product object into selectedProducts
          _.each($scope.availableProducts, function(product){
              if (product.name === productName) {
                  $scope.selectedProducts.push(product);
              }
          });
        }
        
        getSelecetedFeatureFlags($scope.selectedProducts);
        getSelectedComponents($scope.selectedProducts, $scope.components);
    };

    $scope.LPAisSelected = function LPAisSelected(selectedProducts) {
        for (var i = 0; i < selectedProducts.length; i++) {
            if (selectedProducts[i].name === "Lead Prioritization") {
                return true;
            }
        }
        return false;
    };
    
    function getSelecetedFeatureFlags(selectedProducts) {
        $scope.selectedFeatureFlags = [];
        _.each(selectedProducts, function(selectedProduct){
            $scope.selectedFeatureFlags = $scope.selectedFeatureFlags.concat(selectedProduct.featureFlags);
        });
    }
    
    function getSelectedComponents(selectedProducts, components) {
        $scope.selectedComponents = [];
        _.each(selectedProducts, function(selectedProduct){
            var selectedComponentNames = selectedProduct.components;
            _.each(selectedComponentNames, function(selectedComponentName){
                _.each(components, function(component){
                    if (component.Component === selectedComponentName) {
                        if (!containsObject(component, $scope.selectedComponents)) {
                            $scope.selectedComponents.push(component);
                        }
                    }
                });
            });
        });
        console.log($scope.selectedComponents);
    }
    
    function containsObject(obj, list) {
        var i;
        for (i = 0; i < list.length; i++) {
            if (list[i] === obj) {
                return true;
            }
        }
        return false;
    }

    function constructNewPage() {
        $scope.spaceInfo = {
            properties: {
                displayName: $scope.tenantId,
                description: "A LPA solution for " + $scope.tenantId + " in " + $scope.contractId
            },
            featureFlags: ""
        };

        $scope.tenantInfo = {
            properties: {
                displayName: $scope.tenantId,
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
                    $scope.selectedFeatureFlagMap = JSON.parse($scope.spaceInfo.featureFlags);
                } catch (err) {
                    $scope.selectedFeatureFlagMap = "";
                }
                console.log($scope.selectedFeatureFlagMap);
                console.log($scope.featureFlagDefinitions);
                constructSelectedFeatureFlagsInViewPage();

                ServiceService.GetRegisteredServices().then( function(result2) {
                    if (result2.success) {
                        var allServices = result2.resultObj;
                        console.log($scope.availableProducts);
                        $scope.services = constructSelectedComponentsInViewPage(allServices);
                        $scope.componentsToScan = $scope.services.length;
                        
                        _.each($scope.services, function(service) {
                            if (!$scope.selectedFeatureFlagMap.Dante && service === "Dante") {
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
                                    $scope.selectedComponents.push(component);
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

    function constructSelectedFeatureFlagsInViewPage() {
        $scope.selectedFeatureFlags = [];
        for (var selectedFeatureFlag in $scope.selectedFeatureFlagMap) {
            value = $scope.selectedFeatureFlagMap[selectedFeatureFlag];
            if (!$scope.featureFlagDefinitions.hasOwnProperty(selectedFeatureFlag)) {
                console.warn("FeatureFlag: " + selectedFeatureFlag + " has not been defined.");
            } else {
                var featureFlag = $scope.featureFlagDefinitions[selectedFeatureFlag];
                featureFlag.Value = $scope.selectedFeatureFlagMap[selectedFeatureFlag];
                $scope.selectedFeatureFlags.push(featureFlag);
            }
        }
    }

    function constructSelectedComponentsInViewPage(allServices) {
        if ($scope.availableProducts === null || $scope.availableProducts.length === 0) {
            return allServices;
        }
        
        var toReturn = [];
        var selectedProducts = $scope.spaceConfig.Products;
        _.each($scope.availableProducts, function(availableProduct){
            _.each(selectedProducts, function(selectedProduct){
                if (availableProduct.name === selectedProduct) {
                    toReturn = toReturn.concat(availableProduct.components);
                }
            });
        });
        return toReturn;
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
            '<div class="modal-body"> </div>' +
            '<div class="modal-body">' +
            'Deleting a tenant from this UI only remove it from ZK. ' +
            'To fully delete it, you also need to remove it from eacn component. ' +
            'Are you sure you want to delete the tenant {{ tenantId }} from ZK?' +
            '</div>' +
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

    // get $scope.availableProducts through REST call
    function updateAvailableProducts() {
        ServiceService.GetAvailableProducts().then(function(result) {
            if (result.success) {
                $scope.availableProductsNames = result.resultObj;
                $scope.availableProducts = [];
                _.each($scope.availableProductsNames, function(availableProductName) {
                    var availableProduct = {};
                    availableProduct.name = availableProductName;
                    availableProduct.featureFlags = [];
                    availableProduct.components = [];
                    $scope.availableProducts.push(availableProduct);
                });
            } else {
                console.log("Getting available products service error");
            }
        });
    }

    function getAvailableFeatureFlags() {
        ServiceService.GetFeatureFlagDefinitions().then(function(result) {
            if (result.success) {
                $scope.featureFlagDefinitions = result.resultObj;
                parseFeatureFlagDefinitions($scope.featureFlagDefinitions);
            } else {
                console.log("Getting available feature flags service error");
            }
        });
    }
    
    function parseFeatureFlagDefinitions(featureFlagDefinitions) {
        if (featureFlagDefinitions === null) {
            return;
        } else {
            _.each(featureFlagDefinitions, function(featureFlag){
                // initilize its value
                featureFlag.Value = defaultFeatureFlagValue(featureFlag);
                if (featureFlag.AvailableProducts !== null) {
                    _.each(featureFlag.AvailableProducts, function(product){
                        _.each($scope.availableProducts, function(availableProduct){
                            if (availableProduct.name === product) {
                                availableProduct.featureFlags.push(featureFlag);
                            }
                        });
                    });
                }
            });
        }
        console.log("parseFeatureFlagDefinitions" + $scope.availableProducts);
    }
    
    function defaultFeatureFlagValue(featureFlag) {
        if (featureFlag.DisplayName === "Dante") {
            return true;
        }
        if (!featureFlag.Configurable) {
            return true;
        }
        return false;
    }
    
    function getAvailableComponents() {
        ServiceService.GetRegisteredServicesWithAssociatedProducts().then( function(result) {
            if (result.success) {
                $scope.servicesWithProducts = result.resultObj;
                _.each($scope.availableProducts, function(availableProduct){
                  var componentNames = Object.keys($scope.servicesWithProducts);
                  for (var i = 0; i < componentNames.length; i++) {
                      var componentName = componentNames[i];
                      var products = $scope.servicesWithProducts[componentName];
                      for (var j = 0; j < products.length; j++) {
                          if (products[j] === availableProduct.name) {
                              availableProduct.components.push(componentName);
                          }
                      }
                  }
             });
            }
            else {
                console.log("Getting available components service error");
            }
        });
        console.log("getAvailableComponents " + $scope.availableProducts);
    }

    function updateSpaceConfigurationOptions() {
        ServiceService.GetSpaceConfigOptions().then(function(result) {
            if(result.success) {
                var options = result.resultObj;
                _.each(options.Nodes, function(node){
                    switch (node.Node.substring(1)) {
                        case "DL_Address":
                            $scope.availableDLAddresses = node.Options;
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


