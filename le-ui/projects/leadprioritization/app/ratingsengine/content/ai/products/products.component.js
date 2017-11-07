angular.module('lp.ratingsengine.ai.products', [])
    .controller('RatingsEngineAIProducts', function ($q, $timeout, $state, $stateParams, $scope, RatingsEngineAIStore, RatingsEngineAIService, RatingsEngineStore) {
        var vm = this;

        angular.extend(vm, {
            test: true,
            products: [],
            rowsPerPage: 10,
            page: 0,
            maxpages: 10,
            productsSelected: {}

        });

        vm.init = function () {
            console.log('Products initialized');
            //vm.setValidation('products', true);
            vm.loadNextPage();
            vm.validateNextStep();
        }

        vm.setValidation = function (type, validated) {
            RatingsEngineStore.setValidation(type, validated);
        }

        vm.setRowsPerPage = function (rows) {
            alert(rows);
        }

        vm.loadNextPage = function () {
            if (vm.page < vm.maxpages) {
                vm.page++;
                vm.loadProducts(vm.page);
            }
        }

        vm.loadPreviousePage = function () {
            if (vm.page > 1) {
                vm.page--;
                vm.loadProducts(vm.page);
            }
        }

        vm.loadProducts = function (page) {
            RatingsEngineAIService.getProducts(page).then(function (data) {
                vm.products = [];
                vm.products = data;
                for (var i = 0; i < vm.products.length; i++) {
                    vm.products[i]['selected'] = RatingsEngineAIStore.isProductSelected(vm.products[i].id);
                }
            });
        }

        vm.selectProduct = function (index) {
            vm.products[index]['selected'] = (vm.products[index]['selected'] == undefined ? true : !vm.products[index]['selected']);
            var productId = vm.products[index].id;
            RatingsEngineAIStore.selectProduct(productId, vm.products[index].name);
            vm.validateNextStep();
        }

        vm.validateNextStep = function(){
            if(RatingsEngineAIStore.getProductsSelectedCount() > 0){
                vm.setValidation('products', true);
            } else {
                vm.setValidation('products', false);
            }
        }
    
        vm.getProductsSelectedCount = function() {
            RatingsEngineAIStore.getProductsSelectedCount();
        }

        vm.init();
    });