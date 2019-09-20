angular.module('lp.ratingsengine.wizard.products', [
    'mainApp.appCommon.directives.formOnChange'
])
.controller('RatingsEngineProducts', function (
    $scope, $stateParams, $timeout, $filter, RatingsEngineStore, RatingsEngineService, Products, PeriodType, RatingEngine) {
        var vm = this;
        angular.extend(vm, {
            products: $filter('orderBy')(Products, 'ProductName'),
            currentPage: 1,
            pageSize: 10,
            productsCount: 0,
            productsSelected: {},
            sortBy: 'ProductName',
            showPagination: true,
            selectedAll: false,
            engineType: $stateParams.engineType,
            configFilters: {},
            purchasedBeforePeriod: getPurchasedBeforePeriod(),
            periodType: PeriodType.ApsRollingPeriod + '(s)',
            ratingEngine: RatingEngine,
            productCoverage: {},
            productCoverageCounts: {},
            errorProductsMap: new Set(),
            showCounts: false
        });

        // $scope.$watch('vm.search', function(newValue, oldValue) {
        //     if(vm.search || oldValue) {
        //         vm.currentPage = 1;
        //         var products = vm.filteredProductsList.slice(0, 10);
        //         console.log(JSON.stringify(products));
        //         vm.getProductCoverage(vm.ratingEngine, products);
        //     }
        // });

        var timeout = $timeout(function(){});

        vm.handleSearch = function() {
            $timeout.cancel(timeout); //cancel the last timeout
            timeout = $timeout(function(){
                vm.currentPage = 1;
                var products = vm.filteredProductsList.slice(0, 10);
                vm.getProductCoverage(vm.ratingEngine, products);
            }, 1000);
        };

        $scope.$watch('vm.currentPage', function(newValue, oldValue) {
            var products = vm.getProductsToQuery();
            vm.getProductCoverage(vm.ratingEngine, products);
        });

    vm.init = function () {

        vm.getSelectedProducts();

        vm.filteredProducts = vm.products.slice(0, 10);
        vm.productsCount = vm.products.length;

        vm.validateNextStep();

        if (vm.engineType === 'CROSS_SELL_REPEAT_PURCHASE') {
            vm.resellFormOnChange(); 
        } else {
            vm.purchasedBeforePeriod = null;
        };

    }

    vm.getProductsToQuery = function() {
        var start = (vm.currentPage - 1) * vm.pageSize;
        var end = start + vm.pageSize;

        var products = vm.filteredProductsList ? vm.filteredProductsList : vm.products;
        return products.slice(start, end);
    }

    vm.getSelectedProducts = function() {
        if($stateParams.rating_id) {
            RatingsEngineStore.getRating($stateParams.rating_id).then(function(rating){
                var selectedTargetProducts = rating.latest_iteration.AI.advancedModelingConfig.cross_sell.targetProducts;

                if(selectedTargetProducts !== null){

                    angular.forEach(selectedTargetProducts, function(value, key) {
                        var product = Products.filter(function( product ) {
                          return product.ProductId === value;
                        });
                        product[0].Selected = true;

                        var productId = product[0].ProductId,
                            productName = product[0].ProductName;
                        if(!RatingsEngineStore.productsSelected[productId]){
                            RatingsEngineStore.selectProduct(productId, productName);
                        };
                    });
                    vm.productsSelected = RatingsEngineStore.getProductsSelected();
                    vm.validateNextStep();
                } else {
                    RatingsEngineStore.clearSelection();
                    vm.productsSelected = {};
                }
            });
        }
    }

    vm.getTotalProductsCount = function() {
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
        
        vm.productsSelected = RatingsEngineStore.getProductsSelected();
        vm.validateNextStep();
        
        // Uncomment when ability to select all products is available
        // if (Object.keys(vm.productsSelected).length === vm.products.length) {
        //     vm.selectedAll = true;
        // } else {
        //     vm.selectedAll = false;
        // }
    }

    vm.clearProductCoverageAndValidate = function() {
        vm.validateNextStep();
        vm.resellFormOnChange();
        vm.getProductCoverage(vm.ratingEngine, vm.getProductsToQuery());
    }

    vm.validateNextStep = function () {
        if (Object.keys(vm.productsSelected).length > 0 && ( (vm.purchasedBeforePeriod != null && vm.purchasedBeforePeriod >= 0) || vm.engineType === 'CROSS_SELL_FIRST_PURCHASE') ) {
            vm.setValidation('products', true);
        } else {
            vm.setValidation('products', false);
        }
    }

    vm.setValidation = function (type, validated) {
        RatingsEngineStore.setValidation(type, validated);
    }

    vm.resellFormOnChange = function(){
        vm.configFilters.PURCHASED_BEFORE_PERIOD = {
            "configName": "PURCHASED_BEFORE_PERIOD",
            "criteria": "PRIOR_ONLY",
            "value": vm.purchasedBeforePeriod
        };

        RatingsEngineStore.setConfigFilters(vm.configFilters);
    };

    vm.getSelectedCount = function () {
        return Object.keys(vm.productsSelected).length;
    }

    function getPurchasedBeforePeriod() {
        var configFilters = RatingsEngineStore.getConfigFilters();

        if (configFilters && configFilters.PURCHASED_BEFORE_PERIOD) {
            return configFilters.PURCHASED_BEFORE_PERIOD.value;
        } else {
            return 6;
        }
    }

    vm.getPurchasePeriodToQuery = function() {
        return vm.purchasedBeforePeriod == null ? 0 : vm.purchasedBeforePeriod;
    }

    vm.getProductCoverage = function(ratingEngine, filteredList) {
        var purchasedBeforePeriod = vm.purchasedBeforePeriod == null ? 0 : vm.purchasedBeforePeriod;
        if (!vm.productCoverageCounts.hasOwnProperty(purchasedBeforePeriod)) {
            vm.productCoverageCounts[purchasedBeforePeriod] = {};
        };
        var filteredProducts = filteredList.filter(function(value, index) {
            return vm.productCoverageCounts[purchasedBeforePeriod] && !vm.productCoverageCounts[purchasedBeforePeriod].hasOwnProperty(value.ProductId);
        });
        vm.errorProductsMap = new Set();
        if (filteredProducts.length > 0) {
            RatingsEngineStore.getProductCoverage(ratingEngine, filteredProducts, vm.purchasedBeforePeriod).then(function (result) {
                for (var productId in result.ratingModelsCoverageMap) {
                    if (!vm.productCoverageCounts[purchasedBeforePeriod].hasOwnProperty(productId)) {
                        vm.productCoverageCounts[purchasedBeforePeriod][productId] = result.ratingModelsCoverageMap[productId].unscoredAccountCount;
                    }
                }
                for (var productId in result.errorMap) {
                    vm.errorProductsMap.add(productId)
                }
            })
        }
    }

    vm.init();

});