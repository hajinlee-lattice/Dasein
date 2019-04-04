angular.module('common.attributes.categories', [])
.component('attrCategories', {
    templateUrl: '/components/datacloud/attributes/categories/categories.component.html',
    bindings: {
        categories: '<',
        store: '<',
        callback: '&'
    },
    controller: function ($state, $stateParams, StateHistory, DataCloudStore) {
        var vm = this;

        vm.current = 1;
        vm.pagesize = 6;
        vm.catlength = 1;

        vm.$onInit = function() {
            vm.params = $stateParams;

            vm.category = vm.store.get('category');
            vm.catmap = vm.pruneEmptyCategories(angular.copy(vm.categories));
            vm.categories = Object.keys(vm.catmap);
            vm.catlength = vm.categories.length;

            var categoriesSortOrder = DataCloudStore.getCategoriesSortOrder();
            vm.categories = vm.categories.sort(function(a, b){
              return categoriesSortOrder[a] - categoriesSortOrder[b];
            });

            var index = vm.categories.indexOf(vm.category) + 1;

            vm.current = Math.ceil(index / vm.pagesize);
        };

        vm.pruneEmptyCategories = function(catmap) {
            Object.keys(catmap).forEach(function(name) {
                var count = catmap[name];

                if (count === 0) {
                    delete catmap[name];
                }
            });

            return catmap;
        };

        vm.click = function(category) {
            if (!vm.params.section) {
                vm.callback({category: category});
            } else {
                ShowSpinner('Loading ' + category + ' Data', 'div.attr-results-container');

                $state.go('.', { 
                    section: vm.params.section, 
                    category: category, 
                    subcategory: vm.params.subcategory 
                });
            }
            
        };

        vm.categoryIcon = function(category) {
            var path = '/assets/images/enrichments/subcategories/', icon;

            category = vm.subcategoryRenamer(category, '');
            icon = category + '.png';

            return path + icon;
        };

        vm.categoryClass = function(category) {
            category = 'category-' + category.toLowerCase().replace(/\s/g, "-");
            return category;
        };

        vm.subcategoryRenamer = function(string, replacement) {
            if (string) {
                replacement = replacement || '';

                return string.toLowerCase().replace(/\W+/g, replacement);
            }

            return '';
        };

        vm.getTo = function() {
            return StateHistory.lastToParams();
        };

        vm.getFrom = function() {
            return StateHistory.lastFromParams();
        };

        vm.isActive = function(category) {
            if (!vm.params.section) {
                return (vm.store.get('category') == category) ? true : false;
            } else {
                var x = vm.category == category;
                var y = vm.getTo().category == category;
                var z = vm.getFrom().category != category || vm.getTo().category == category;

                return (x || y) && z;    
            }
        };
    }
});