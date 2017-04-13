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
                if(vm.lookupMode) {
                    order = [ '-HighlightHighlighted', '-ImportanceOrdering', '-Value' ];
                } else if(vm.section == 'segment.analysis') {
                    order = [ 'SegmentChecked', '-Value' ];
                } else {
                    order = [ '-HighlightHighlighted', '-Value' ];
                }
                // remove highlighting
                if(!vm.showHighlighting()) {
                    order = order.filter(function(item){
                        return item != '-HighlightHighlighted' && item != 'HighlightHighlighted'
                    });
                }
                return order;
                /*
                ret = vm.lookupMode 
                ? [ '-HighlightHighlighted', '-ImportanceOrdering', '-Value' ]
                    : vm.section == 'segment.analysis' 
                        ? [ 'SegmentChecked', '-Value'  ]
                        : [ '-HighlightHighlighted', '-Value' ];
                return ret;
                */
            }

            vm.categoryClick = function(category, $event) {
                var target = angular.element($event.target),
                    currentTarget = angular.element($event.currentTarget);
                if(target.closest("[ng-click]:not(.ignore-ngclick)")[0] !== currentTarget[0]) {
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
            
            vm.displayAttributeValue = function(attribute, property) {
                var property = property || 'Lbl',
                    enrichmentKey = attribute.Attribute || attribute.FieldName,
                    index = vm.enrichmentsMap[enrichmentKey],
                    enrichment = vm.enrichments[index],
                    stats = (vm.cube.Stats[enrichmentKey] && vm.cube.Stats[enrichmentKey].RowStats && vm.cube.Stats[enrichmentKey].RowStats.Bkts && vm.cube.Stats[enrichmentKey].RowStats.Bkts.List ? vm.cube.Stats[enrichmentKey].RowStats.Bkts.List : null),
                    stat = (stats && stats.length ? stats[0] : null),
                    segmentRangeKey = null;
                if(stat && stat.Range) {
                    segmentRangeKey = vm.makeSegmentsRangeKey(enrichment,stat.Range);
                }
                //console.log(vm.cube.Stats[enrichmentKey].RowStats.Bkts.List);
                if(stats && stats.length > 1) {
                    for(var i in stats) {
                        if(stats[i] && stats[i].Range) {
                            if(vm.segmentAttributeInputRange[vm.makeSegmentsRangeKey(enrichment,stats[i].Range)]) {
                                stat = stats[i];
                                break;
                            }
                        }

                    }
                }
                if(stat && stat[property]) {
                    if(property === 'Lift') {
                        return stat[property].toFixed(1) + 'x';
                    }
                    return stat[property];
                }
            }
        }
    };
});