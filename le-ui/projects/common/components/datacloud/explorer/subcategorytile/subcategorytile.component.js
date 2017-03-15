angular
.module('common.datacloud.explorer.subcategorytile', [])
.directive('explorerSubcategoryTile',function() {
    return {
        restrict: 'A',
        scope: {
            vm: '=',
            count: '=',
            subcategory: '='
        },
        controllerAs: 'vm',
        templateUrl: '/components/datacloud/explorer/subcategorytile/subcategorytile.component.html',
        controller: function ($scope, $document, $timeout, $interval, DataCloudStore) {
            var vm = $scope.vm;
            
            angular.extend(vm, { });


            vm.inSubcategory = function(enrichment){
                var category = vm.selected_categories[enrichment.Category],
                    subcategories = (category && category['subcategories'] ? category['subcategories'] : []),
                    subcategory = enrichment.Subcategory;

                if (enrichment.DisplayName && !subcategories.length) { // for case where this is used as a | filter in the enrichments ngRepeat on initial state
                    return true;
                }

                if (!subcategories.length) {
                    return false;
                }

                var selected = (typeof category === 'object' && subcategories.indexOf(subcategory) > -1);
                return selected;
            }

            vm.subcategoryIcon = function(category, subcategory){
                var path = '/assets/images/enrichments/subcategories/',
                    category = vm.subcategoryRenamer(category),
                    subcategory = vm.subcategoryRenamer(subcategory),
                    icon = category + (subcategory ? '-'+subcategory : '') + '.png';

                return path + icon;
            }

            vm.subcategoryClick = function(subcategory, $event) {
                var target = angular.element($event.target),
                    currentTarget = angular.element($event.currentTarget);

                if(target.closest("[ng-click]")[0] !== currentTarget[0]) {
                    // do nothing, user is clicking something with it's own click event
                } else {
                    vm.setSubcategory((vm.subcategory === subcategory ? '' : subcategory));
                    vm.metadata.current = 1;
                    vm.updateStateParams();
                }
            }
        }
    };
});