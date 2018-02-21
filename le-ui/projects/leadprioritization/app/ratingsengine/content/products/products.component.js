angular.module('lp.ratingsengine.wizard.products', [
    'mainApp.appCommon.directives.formOnChange'
])
.controller('RatingsEngineProducts', function (
    $scope, $stateParams, $timeout, RatingsEngineStore, RatingsEngineService, Products, GetSelectedProducts) {
        var vm = this;
        angular.extend(vm, {
            products: Products,
            currentPage: 1,
            pageSize: 10,
            productsCount: 0,
            productsSelected: GetSelectedProducts,
            sortBy: 'ProductName',
            showPagination: true,
            selectedAll: false,
            engineType: $stateParams.engineType,
            modelingConfigFilters: {},
            purchasedBeforePeriod: '6'
        });

        $scope.$watch('vm.search', function(newValue, oldValue) {
        if(vm.search || oldValue) {
            vm.currentPage = 1;
        }
    });

    vm.init = function () {

        RatingsEngineStore.setCachedProducts(vm.products);

        vm.filteredProducts = vm.products.slice(0, 10);
        vm.productsCount = vm.products.length;

        vm.validateNextStep();

        if (vm.engineType === 'CROSS_SELL_RETURNING_PURCHASE') {
            vm.resellFormOnChange();    
        };

    }

    vm.getTotalProductsCount = function () {
        return vm.productsCount;
    }

    vm.selectAll = function(){
        if (vm.selectedAll) {
            vm.selectedAll = true;
            RatingsEngineStore.selectAllProducts(vm.products);
            vm.products.forEach(function(product){
                product.Selected = true;
            });
        } else {
            vm.selectedAll = false;
            vm.products.forEach(function(product){
                product.Selected = false;
            });
            RatingsEngineStore.clearSelection();
        }

        vm.productsSelected = RatingsEngineStore.getProductsSelected();

        vm.validateNextStep();
    }
    vm.selectProduct = function (productId, index) {

        var product = vm.products.filter(function( product ) {
          return product.ProductId === productId;
        });

        RatingsEngineStore.selectProduct(productId, product[0].ProductName);
        
        vm.validateNextStep();
        vm.productsSelected = RatingsEngineStore.getProductsSelected();
        
        if (RatingsEngineStore.getProductsSelectedCount() === vm.products.length) {
            vm.selectedAll = true;
        } else {
            vm.selectedAll = false;
        }
    }

    vm.validateNextStep = function () {
        if (RatingsEngineStore.getProductsSelectedCount() > 0) {
            vm.setValidation('products', true);
        } else {
            vm.setValidation('products', false);
        }
    }

    vm.setValidation = function (type, validated) {
        RatingsEngineStore.setValidation(type, validated);
    }

    vm.resellFormOnChange = function(){
        vm.modelingConfigFilters.PURCHASED_BEFORE_PERIOD = {
            "configName": "PURCHASED_BEFORE_PERIOD",
            "criteria": "PRIOR",
            "value": parseInt(vm.purchasedBeforePeriod)
        };

        RatingsEngineStore.setModelingConfigFilters(vm.modelingConfigFilters);
    };

    vm.getProductsSelectedCount = function () {        
        return RatingsEngineStore.getProductsSelectedCount();
    }

    vm.init();

});