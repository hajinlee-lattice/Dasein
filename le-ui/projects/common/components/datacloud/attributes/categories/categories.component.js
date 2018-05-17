angular.module('common.attributes.categories', [])
.component('attrCategories', {
    templateUrl: '/components/datacloud/attributes/categories/categories.component.html',
    bindings: {
        categories: '<'
    },
    controller: function ($state, $stateParams, StateHistory) {
        var vm = this;

        vm.current = 1;
        vm.pagesize = 7;
        vm.catlength = 1;

        vm.$onInit = function() {
            vm.params = $stateParams;
            vm.category = vm.params.category;
            vm.catmap = angular.copy(vm.categories);
            vm.categories = Object.keys(vm.categories);
            vm.catlength = vm.categories.length;
        };

        vm.click = function(category) {
            ShowSpinner('Loading ' + category + ' Data', 'div.attr-results-container')
            
            $state.go('.', { 
                section: vm.params.section, 
                category: category, 
                subcategory: vm.params.subcategory 
            });
        };

        vm.categoryIcon = function(category) {
            var path = '/assets/images/enrichments/subcategories/', icon;

            category = vm.subcategoryRenamer(category, '');
            icon = category + '.png';

            return path + icon;
        };

        vm.categoryClass = function(category) {
            category = 'category-' + category.toLowerCase().replace(' ','-');
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
            var x = vm.params.category == category;
            var y = vm.getTo().category == category;
            var z = vm.getFrom().category != category || vm.getTo().category == category;

            return (x || y) && z;
        };
    }
});