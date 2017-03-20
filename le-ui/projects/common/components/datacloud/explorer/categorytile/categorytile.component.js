angular
.module('common.datacloud.explorer.categorytile', [])
.directive('explorerCategoryTile',function() {
    return {
        restrict: 'A',
        scope: {
            vm: '=',
            count: '=',
            category: '='
        },
        controllerAs: 'vm',
        templateUrl: '/components/datacloud/explorer/categorytile/categorytile.component.html',
        controller: function ($scope, $document, $timeout, $interval, DataCloudStore) {
            var vm = $scope.vm;
            
            angular.extend(vm, { });

            vm.setCategory = function(category) {
                vm.category = category;
                DataCloudStore.setMetadata('category', category);
            }

            vm.categoryClass = function(category) {
                var category = 'category-' + category.toLowerCase().replace(' ','-');
                return category;
            }

            vm.categoryIcon = function(category) {
                var path = '/assets/images/enrichments/subcategories/',
                    category = vm.subcategoryRenamer(category, ''),
                    icon = category + '.png';

                return path + icon;
            }

            vm.categoryOrderBy = function(test) {
                return vm.lookupMode 
                    ? [ '-HighlightHighlighted', '-ImportanceOrdering', '-Value' ]
                    : [ '-HighlightHighlighted', '-Value' ];
            }

            vm.categoryClick = function(category, $event) {
                var target = angular.element($event.target),
                    currentTarget = angular.element($event.currentTarget);
                if(target.closest("[ng-click]")[0] !== currentTarget[0]) {
                    // do nothing, user is clicking something with it's own click event
                } else {
                    var category = category || '';
                    if(vm.subcategory && vm.category == category) {
                        vm.setSubcategory('');
                        if(vm.subcategoriesExclude.indexOf(category) >= 0) { // don't show subcategories
                            vm.setSubcategory(vm.subcategories[category][0]);
                        }
                    } else if(vm.category == category) {
                        vm.setSubcategory('');
                        //vm.category = '';
                    } else {
                        vm.setSubcategory('');
                        if(vm.subcategoriesExclude.indexOf(category)) {
                            vm.setSubcategory(vm.subcategories[category][0]);
                        }
                        vm.setCategory(category);

                        vm.filterEmptySubcategories();
                    }
                    vm.metadata.current = 1;
                    vm.updateStateParams();
                }
            }
        }
    };
});