angular.module('lp.ratingsengine.ai.products', [])
    .controller('RatingsEngineAIProducts', function ($scope, RatingsEngineAIStore, RatingsEngineAIService, RatingsEngineStore, Products) {
        var vm = this;
        angular.extend(vm, {
            totalProducts: (function () {
                var max = Products.length;
                var ret = [];
                for (var i = 0; i < max; i++) {
                    ret.push({ 'id': Products[i].ProductId, 'displayName': Products[i].ProductName });
                }
                return ret;
            })(),
            products: [],
            rowsPerPage: 5,
            productsCount: 0,
            page: 0,
            maxpages: 0,
            productsSelected: {}

        });

        vm.init = function () {
            vm.productsCount = vm.totalProducts.length;
            vm.calculateMaxPages();
            vm.loadNextPage();
            vm.validateNextStep();

        }
        /**
         * Initialize the max number of mages available based on
         * the number of product and the number of rows per page
         */
        vm.calculateMaxPages = function () {
            if (vm.productsCount > 0) {
                if (vm.productsCount == vm.rowsPerPage) {
                    vm.maxpages = 1;
                } else {
                    vm.maxpages = Math.ceil(vm.productsCount / vm.rowsPerPage);
                }
            }
        }
        /**
         * Fetch product for next page
         */
        vm.loadNextPage = function () {
            if (vm.page < vm.maxpages) {
                vm.page++;
                vm.loadProducts(vm.page);
            }
        }
        /**
         * Fetch product for previouse page
         */
        vm.loadPreviousePage = function () {
            if (vm.page > 1) {
                vm.page--;
                vm.loadProducts(vm.page);
            }
        }
        /**
         * @param page @type number Page of products to show
         */
        vm.loadProducts = function (page) {
            var from = (page - 1) * vm.rowsPerPage;
            var to = page * vm.rowsPerPage;
            if (to > vm.productsCount) {
                to = vm.productsCount;
            }

            vm.products = [];
            var j = 0;
            for (var i = from, j = 0; i < to; i++, j++) {
                vm.products[j] = angular.copy(vm.totalProducts[i]);
                vm.products[j]['selected'] = RatingsEngineAIStore.isProductSelected(vm.products[j].id);
            }
        }

        vm.getTotalProductsCount = function () {
            return vm.productsCount;
        }

        /**
         * @param index index in the visible page of the grid
         */
        vm.selectProduct = function (index) {
            vm.products[index]['selected'] = (vm.products[index]['selected'] == undefined ? true : !vm.products[index]['selected']);
            var productId = vm.products[index].id;
            RatingsEngineAIStore.selectProduct(productId, vm.products[index].displayName);
            vm.validateNextStep();
        }
        /**
         * @returns the number of product selected
         */
        vm.getProductsSelectedCount = function () {
            return RatingsEngineAIStore.getProductsSelectedCount();
        }
        /**
         * Method used to validate the next step for the wizard
         */
        vm.validateNextStep = function () {
            if (RatingsEngineAIStore.getProductsSelectedCount() > 0) {
                vm.setValidation('products', true);
            } else {
                vm.setValidation('products', false);
            }
        }
        /**
         * Enable/Disable the next step of the wizard
         * @argument type @type string This value has to be equal to the value which is inside the RatingsEngineStore.validation
         * @argument validated @type boolean Enable or disable the next step
         * 
         */
        vm.setValidation = function (type, validated) {
            RatingsEngineStore.setValidation(type, validated);
        }

        vm.init();
    });