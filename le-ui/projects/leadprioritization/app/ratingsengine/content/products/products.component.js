angular.module('lp.ratingsengine.wizard.products', [
    'mainApp.appCommon.directives.formOnChange'
])
.controller('RatingsEngineProducts', function (
    $scope, RatingsEngineStore, RatingsEngineService, Products) {
        var vm = this;
        angular.extend(vm, {
            products: Products,
            currentPage: 1,
            pageSize: 10,
            productsCount: 0,
            productsSelected: {},
            sortBy: 'ProductName',
            showPagination: true,
            selectedAll: false,
            type: RatingsEngineStore.getType(),
            modelingConfigFilters: {},
            purchasedBeforePeriod: '6'
        });

        $scope.$watch('vm.search', function(newValue, oldValue) {
        if(vm.search || oldValue) {
            vm.currentPage = 1;
        }
    });

    vm.init = function () {

        // console.log(vm.products);

        RatingsEngineStore.setCachedProducts(vm.products);

        vm.filteredProducts = vm.products.slice(0, 10);
        vm.productsCount = vm.products.length;

        vm.validateNextStep();

        if (vm.type.engineType === 'CROSS_SELL_RETURNING_PURCHASE') {
            vm.resellFormOnChange();    
        };

    }

    vm.getTotalProductsCount = function () {
        return vm.productsCount;
    }

    vm.selectAll = function(){
        if (vm.selectedAll) {
            vm.selectedAll = true;
            angular.forEach(vm.products, function(product, index) {
                if(!RatingsEngineStore.isProductSelected(product.ProductId)){
                    vm.selectProduct(index, product.ProductId);
                }
            });
        } else {
            vm.selectedAll = false;
            angular.forEach(vm.products, function(product, index) {
                if(RatingsEngineStore.isProductSelected(product.ProductId)){
                    vm.selectProduct(index, product.ProductId);
                }
            });
            RatingsEngineStore.clearSelection();
        }

        vm.productsSelected = RatingsEngineStore.getProductsSelected();

        vm.validateNextStep();
    }
    vm.selectProduct = function (index, productId) {

        vm.products[index]['Selected'] = (vm.products[index]['Selected'] == undefined ? true : !vm.products[index]['Selected']);

        RatingsEngineStore.selectProduct(productId, vm.products[index].ProductName);
        
        vm.validateNextStep();
        vm.productsSelected = RatingsEngineStore.getProductsSelected();
        
        if (RatingsEngineStore.getProductsSelectedCount() === vm.products.length) {
            vm.selectedAll = true;
        } else {
            vm.selectedAll = false;
        }
    }

    vm.getProductsSelectedCount = function () {
        return RatingsEngineStore.getProductsSelectedCount();
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

    vm.init();
});