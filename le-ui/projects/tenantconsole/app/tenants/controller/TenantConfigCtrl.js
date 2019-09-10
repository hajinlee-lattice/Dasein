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

app.controller('TenantConfigCtrl', function($scope, $rootScope, $timeout, $state, $stateParams, $uibModal, $interval, _, TenantService, TenantUtility, ServiceService) {
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
    if ($stateParams.new) { $scope.new = $stateParams.new === "true"; }
    if ($stateParams.readonly) { $scope.readonly = $stateParams.readonly === "true"; }
    if ($stateParams.listenState) { $scope.listenState = $stateParams.listenState === "true"; }

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
    $scope.twoLPsAreSelected = false;
    $scope.LPA3isSelected = false;
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
    $scope.hasGa_Dev = false;
    $scope.componentsWithoutGA_Dev = [];
    $scope.componentsWithGA_Dev = [];


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
        }, 10000);
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

        var passedCheck = preSaveCheck();
        if (!passedCheck) {
            return;
        }
        popInstallConfirmationModal();
    };

    $scope.onDeleteClick = function(){ popDeleteConfirmationModal(); };

    $scope.$on("CALC_DERIVED", function(evt, data) {
        if ($state.current.name !== "TENANT.CONFIG") {
            var derivation = data.derivation;
            var derivedValue = TenantUtility.calcDerivation($scope.components, derivation, $scope);
            data.callback(derivedValue);
        }
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
        LPASelected = false;
        $scope.twoLPsAreSelected = false;
        var selectedLPAnum = 0;
        for (var i = 0; i < selectedProducts.length; i++) {
            if (selectedProducts[i].name === ( "Lead Prioritization")) {
                LPASelected = true;
                selectedLPAnum += 1;
            } else if (selectedProducts[i].name === ( "Lead Prioritization 3.0")) {
                selectedLPAnum += 1;
                $scope.LPA3isSelected = true;
            }
        }
        if (selectedLPAnum === 2) {
            $scope.twoLPsAreSelected = true;
        }
        return LPASelected;
    };

    $scope.changeTenantType = function() {
        if ($scope.selectedProductNames.indexOf("Customer Growth") >= 0) {
            $scope.selectedFeatureFlags.forEach(function(flag) {
                if (flag.DisplayName === 'AllowAutoSchedule') {
                    flag.Value = setAutoSchedulingFlagValue();
                }
            });
        }
        modifyComponentsAccordingToType();
        // reselect components after the type change event
        getSelectedComponents($scope.selectedProducts, $scope.components);
    };

   function modifyComponentsAccordingToType() {
    	 if ($scope.tenantInfo.properties.tenantType === "CUSTOMER" || $scope.tenantInfo.properties.tenantType === "STAGING") {
         	$scope.components = $scope.componentsWithoutGA_Dev;
         } else {
        	 $scope.components = $scope.componentsWithGA_Dev;
         }
         if ($scope.tenantInfo.properties.tenantType === "POC") {
             _.each($scope.components, function (component) {
                if (component.Component === "CDL") {
                    _.each(component.Nodes, function(node) {
                        if (node.Node === "AccountQuotaLimit") {
                        node.Data = 1000000;
                        }
                    });
                }
             });
         }
         console.log($scope.components);
    }
    function setAutoSchedulingFlagValue() {
        return $scope.tenantInfo.properties.tenantType === "CUSTOMER" &&
                                    $scope.selectedProductNames.length === 2 && $scope.selectedProductNames.indexOf("Lead Prioritization 3.0") >= 0 &&
                                    $scope.selectedProductNames.indexOf("Customer Growth") >= 0;
    }

    function getSelecetedFeatureFlags(selectedProducts) {
        var featureFlags = [];
        var flagNames = [];
        selectedProducts.forEach(function(selectedProduct){
            selectedProduct.featureFlags.forEach(function(flag) {
                if (!flagNames.includes(flag.DisplayName)) {
                    featureFlags.push(flag);
                    flagNames.push(flag.DisplayName);
                }
                if (flag.DisplayName === "AllowAutoSchedule") {
                    flag.Value = setAutoSchedulingFlagValue();
                }
            });
        });
        $scope.selectedFeatureFlags = featureFlags;
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
                description: "A Lattice tenant under the contract " + $scope.contractId,
                status: "ACTIVE",
                tenantType: "CUSTOMER"
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
                            if ($scope.defaultConfigScaned === $scope.services.length) {
                                $scope.loading = false;
                                $scope.componentsWithGA_Dev = angular.copy($scope.components);
                                _.each($scope.components, function (component) {
                             		if (component.Component === "PLS") {
                     	                _.each(component.Nodes, function(node) {
                     	                	if (node.Node === "SuperAdminEmails") {
                     	                		var superAdminEmails = node.Data;
                     	                		node.Data = node.Data.replace("\"ga_dev@lattice-engines.com\",", "").replace(",\"ga_dev@lattice-engines.com\"", "").replace("\"ga_dev@lattice-engines.com\"", "");
                     	                    }
                     	                });
                             		}
                                 });
                                $scope.componentsWithoutGA_Dev =  angular.copy($scope.components);
                                modifyComponentsAccordingToType();
                                if ($state.current.name !== "TENANT.CONFIG") {
                                    $timeout(function () {
                                        $rootScope.$broadcast("UPDATE_DERIVED");
                                    }, 500);
                                }
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
                                        if ($scope.listenState) { updateServiceStatus(); }
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
                if (!featureFlag.Deprecated) {
                    featureFlag.Value = $scope.selectedFeatureFlagMap[selectedFeatureFlag];
                    $scope.selectedFeatureFlags.push(featureFlag);
                }
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
        $uibModal.open({
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
            controller: function($scope, $uibModalInstance, data, contractId, tenantId, TenantService){
                $scope.data = data;
                $scope.tenantId = tenantId;
                $scope.contractId = contractId;
                $scope.showErrorMsg = false;

                $scope.submit = function() {
                    $scope.showErrorMsg = false;
                    TenantService.CreateTenant($scope.tenantId, $scope.contractId, $scope.data).then(
                        function(result) {
                            if (result.success) {
                                $uibModalInstance.dismiss();
                                $state.go('TENANT.LIST');
                            } else {
                                $scope.errorMsg = "Adding tenant failed.";
                                $scope.showErrorMsg = true;
                            }
                        }
                    );
                };

                $scope.cancel = function () {
                    $uibModalInstance.dismiss('cancel');
                };
            },
            resolve: {
                data: function () { return $scope.tenantRegisration; },
                tenantId: function () { return $scope.tenantId; },
                contractId: function () { return $scope.contractId; }
            }
        });
    }

    function preSaveCheck() {
         if ($scope.twoLPsAreSelected) {
             alert("Cannot choose two LPA products at the same time!");
             return false;
         } else if ($scope.LPA3isSelected) {
             var atLeastOneSourceFeatureFlagIsChecked = true;
             if ($scope.featureFlags.UseSalesforceSettings === false && $scope.featureFlags.UseMarketoSettings === false &&
                     $scope.featureFlags.UseEloquaSettings === false ) {
                 atLeastOneSourceFeatureFlagIsChecked = false;
             }
             if (!atLeastOneSourceFeatureFlagIsChecked) {
                 alert("At least one source feature flag for LP3.0 should be checked.");
                 return false;
             }
         }
         if (!$scope.tenantInfo.properties.contract) {
         	 alert("Please populate contract.");
             return false;
         }
         return true;
    }

    function popDeleteConfirmationModal() {
        $uibModal.open({
            template: '<div class="modal-header">' +
            '<h3 class="modal-title">Delete tenant</h3></div>' +
            '<div class="modal-body">' +
            'Are you sure you want to delete the tenant {{ tenantId }}?' +
            '<div>Type "DELETE ME" into the the text area below to confirm you want to delete this tenant</div>' +
            '<div><input ng-model="confirm"></div>' +
            '</div>' +
            '<div class="modal-footer">' +
            '<button class="btn btn-primary" ng-disabled="confirm !== deleteMe" ng-click="ok()">YES</button>' +
            '<button class="btn btn-default" ng-click="cancel()">NO</button>' +
            '</div>',
            controller: function($scope, $state, $uibModalInstance, tenantId, contractId, TenantService){
                $scope.tenantId = tenantId;
                $scope.contractId = contractId;
                $scope.confirm = '';
                $scope.deleteMe = 'DELETE ME';

                $scope.ok = function() {
                    if ($scope.confirm !== $scope.deleteMe) {
                        return;
                    }
                    TenantService.DeleteTenant(tenantId, contractId).then(function(result){
                        if (result.success) {
                            $uibModalInstance.dismiss();
                            $state.go('TENANT.LIST');
                        } else {
                            //handle error
                        }
                    });
                };

                $scope.cancel = function () {
                    $uibModalInstance.dismiss();
                };
            },
            resolve: {
                tenantId: function () { return $scope.tenantId; },
                contractId: function () { return $scope.contractId; }
            }
        });
    }

    function updateServiceStatus() {
        var componentsToScan = $scope.selectedComponents.length;
        _.each($scope.selectedComponents, function (component) {
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
                                if ($state.current.name !== "TENANT.CONFIG") {
                                    $scope.$broadcast("UPDATE_DERIVED");
                                }
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
                if (!featureFlag.Deprecated && featureFlag.AvailableProducts !== null) {
                    // initialize its value
                    featureFlag.Value = featureFlag.DefaultValue;
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
                        case "Status":
                            $scope.availableStatuses = node.Options;
                            break;
                        case "TenantType":
                            $scope.availableTenantTypes = node.Options;
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


    //==================================================
    // edit tenant info
    //==================================================

    $scope.isEditingTenantInfo = false;
    $scope.isSavingTenantInfo = false;

    $scope.editTenantInfo = function() {
        $scope.originalTenantStatus = $scope.tenantInfo ? $scope.tenantInfo.properties.status : '';
        $scope.originalTenantType = $scope.tenantInfo ? $scope.tenantInfo.properties.tenantType : '';
        $scope.isEditingTenantInfo = true;
    };

    $scope.saveTenantInfo = function() {
        $scope.isEditingTenantInfo = false;
        $scope.isSavingTenantInfo = true;
        if ($scope.tenantInfo.properties.expiredTime) {
        	$scope.tenantInfo.properties.expiredTime = Date.parse($scope.tenantInfo.properties.expiredTime);
        }
        TenantService.UpdateTenantInfo($scope.contractId, $scope.tenantId, $scope.tenantInfo).then(function(result) {
            $scope.isSavingTenantInfo = false;
            if (result === false) {
                $scope.tenantInfo.properties.status = $scope.originalTenantStatus;
                $scope.tenantInfo.properties.tenantType = $scope.originalTenantType;
            }

        });
    };

    $scope.cancelTenantInfo = function() {
        $scope.tenantInfo.properties.status = $scope.originalTenantStatus;
        $scope.tenantInfo.properties.tenantType = $scope.originalTenantType;
        $scope.isEditingTenantInfo = false;
    };


    //==================================================
    // edit feature flags
    //==================================================
    var featureFlagsBackup = null;
    $scope.isEditingFeatureFlag = false;

    $scope.editFeatureFlags = function () {
        backupFeatureFlags();
        $scope.isEditingFeatureFlag = true;
    };

    $scope.saveFeatureFlags = function () {
        $scope.featureFlagUpdateError = null;

        var flagsToUpdate = {};
        if (featureFlagsBackup) {
            _.each($scope.selectedFeatureFlags, function (featureFlag) {
                if (featureFlagsBackup[featureFlag.DisplayName] !== undefined) {
                    if (featureFlag.Value !== featureFlagsBackup[featureFlag.DisplayName]) {
                        flagsToUpdate[featureFlag.DisplayName] = featureFlag.Value;
                    }
                }
            });
        }

        TenantService.UpdateFeatureFlags($scope.tenantId, flagsToUpdate)
            .then(function(results) {
               $scope.isEditingFeatureFlag = false;
               featureFlagsBackup = null;
            }).catch(function(error) {
                if (error && error.errorMsg) {
                    $scope.featureFlagUpdateError = 'Error updating feature flag. ' + error.errMsg.errorCode + ': '+ error.errMsg.ErrorMsg;
                } else {
                    $scope.featureFlagUpdateError = 'Error updating feature flag';
                }
            });
    };

    $scope.cancelEditFeatureFlags = function () {
        undoFeatureFlagChanges();
        $scope.isEditingFeatureFlag = false;
        $scope.featureFlagUpdateError = null;
    };

    function backupFeatureFlags() {
        featureFlagsBackup = {};
        _.each($scope.selectedFeatureFlags, function (featureFlag) {
            if (featureFlag.Configurable === true) {
                featureFlagsBackup[featureFlag.DisplayName] = featureFlag.Value;
            }
        });
    }

    function undoFeatureFlagChanges() {
        if (featureFlagsBackup) {
            _.each($scope.selectedFeatureFlags, function (featureFlag) {
                if (featureFlagsBackup[featureFlag.DisplayName] !== undefined) {
                    featureFlag.Value = featureFlagsBackup[featureFlag.DisplayName];
                }
            });
            featureFlagsBackup = null;
        }
    }


    $scope.isEditingComponents = false;
    var componentsBackUp = null;

    $scope.editComponents = function () {
        backupComponents();
        $scope.isEditingComponents = true;
    };
    $scope.saveComponents = function () {
         $scope.ComponentsUpdateError = null;

         var componentsToUpdate = {};
         if (componentsBackup) {
             _.each($scope.selectedComponents, function (component) {
                 if (componentsBackup[component.Component] !== undefined) {
                     var copy = componentsBackup[component.Component];
                     var update = {};
                     _.each(component.Nodes, function(node) {
                         if(copy[node.Node] !== undefined) {
                             if (angular.isObject(copy[node.Node])) {
                                 var copy2 = copy[node.Node];
                                 _.each(node.Children, function (node2) {
                                     if (copy2[node2.Node] !== node2.Data) {
                                         update['/'+ node.Node + '/' + node2.Node] = angular.copy(node2.Data);
                                     }
                                 });
                             } else if (copy[node.Node] !== node.Data) {
                                 update['/'+ node.Node] = angular.copy(node.Data);
                             }
                         }
                     });
                     if (!angular.equals(update, {})) {
                         componentsToUpdate[component.Component] = update;
                     }
                 }

             });
         }

         TenantService.UpdateComponents($scope.tenantId, componentsToUpdate)
             .then(function(results) {
                $scope.isEditingComponents = false;
                componentsBackup = null;
             }).catch(function(error) {
                 if (error && error.errorMsg) {
                     $scope.componentsUpdateError = 'Error updating feature flag. ' + error.errMsg.errorCode + ': '+ error.errMsg.ErrorMsg;
                 } else {
                     $scope.componentsUpdateError = 'Error updating feature flag';
                 }
             });
    };
    $scope.cancelComponents = function () {
        undoComponentsChanges();
        $scope.isEditingComponents = false;
        $scope.componentsUpdateError = null;
    };
    function backupComponents() {
        componentsBackup = {};
        _.each($scope.selectedComponents, function (component) {
            var copy = {};
            _.each(component.Nodes, function(node)  {
                var copy2 = {};
                _.each(node.Children, function (node2) {
                    copy2[node2.Node] = angular.copy(node2.Data);
                });
                if (angular.equals(copy2, {})) {
                    copy[node.Node] = angular.copy(node.Data);
                } else {
                    copy[node.Node] = copy2;
                }

            });
            componentsBackup[component.Component]=copy;
        });
    }
    function undoComponentsChanges() {
        if (componentsBackup) {
            _.each($scope.selectedComponents, function (component) {
                if (componentsBackup[component.Component] !== undefined) {
                    var copy = componentsBackup[component.Component];
                    _.each(component.Nodes, function (node) {
                        if (copy[node.Node] !== undefined) {
                            var copy2 = copy[node.Node];
                            if (angular.isObject(copy[node.Node])) {
                                _.each(node.Children, function (node2) {
                                    if (copy2[node2.Node] !== undefined) {
                                        node2.Data = copy2[node2.Node];
                                    }
                                });
                            } else {
                                node.Data = copy[node.Node];
                            }

                        }
                    });
                }
            });
            componentsBackup = null;
        }
    }
});


