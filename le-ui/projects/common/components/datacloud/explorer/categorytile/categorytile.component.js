export default function () {
    return {
        restrict: 'A',
        scope: {
            vm: '=',
            count: '=',
            category: '='
        },
        controllerAs: 'vm',
        templateUrl: '/components/datacloud/explorer/categorytile/categorytile.component.html',
        controller: function ($scope, DataCloudStore, QueryStore, moment) {
            'ngInject';

            var vm = $scope.vm;

            angular.extend(vm, {});

            let setCategory = DataCloudStore.getMetadata('category');
            $scope.$watch(setCategory, function() {
                vm.setCategory(setCategory);
            });

            vm.setCategory = function (category) {
                vm.category = category;
                DataCloudStore.setMetadata('category', category);
            }

            vm.categoryClass = function (category) {
                var category = 'category-' + category.toLowerCase().replace(/\s/g, "-");
                return category;
            }

            vm.categoryIcon = function (category) {
                var path = '/assets/images/enrichments/subcategories/',
                    category = vm.subcategoryRenamer(category, ''),
                    icon = category + '.png';

                return path + icon;
            }

            /**
             * Retrurn the path of the image starting from /assets
             * In case of category === 'latticerating' or 'productspendprofile'
             * it returns the category icon 
             * @param {*} category 
             * @param {*} subcategory 
             */
            vm.subcategoryIcon = function (category, subcategory) {
                var path = '/assets/images/enrichments/subcategories/',
                    category = vm.subcategoryRenamer(category),
                    subcategory = vm.subcategoryRenamer(subcategory),
                    icon = category + (subcategory ? '-' + subcategory : '') + '.png';

                switch (category) {
                    case 'latticeratings': {
                        icon = 'latticeratings.png';
                        break;
                    }
                    case 'productspendprofile': {
                        icon = 'productspendprofile.png';
                        break;
                    }
                }
                return path + icon;
            }

            vm.categoryStartFrom = function () {
                var size = vm.category ? vm.categorySize : vm.pagesize,
                    current = vm.metadata.currentCategory - 1,
                    items = vm.categoriesMenu,
                    length = items ? items.length : 0,
                    result = (current * size + size) > length
                        ? length - size
                        : current * size;

                return (result < 0 ? 0 : result);
            }

            vm.categoryLimitTo = function () {
                return vm.category ? vm.categorySize : vm.pagesize;
            }

            vm.categoryOrderBy = function (category, subcategory, attr) {
                var YesCategories = [
                    'Technology Profile',
                    'Website Profile',
                    'Product Spend Profile'
                ];

                if (vm.section == 'segment.analysis') {
                    var order = [];

                    if (category && YesCategories.indexOf(category) >= 0) {
                        order.push(function (attribute) {
                            return attribute.TopBkt && attribute.TopBkt.Lbl == 'Yes' ? -1 : 1;
                        });
                    }

                    order = order.concat(['!TopBkt', '!ImportanceOrdering', '-ImportanceOrdering', '-TopBkt.Cnt', '-Value']);
                } else {
                    var order = !vm.showHighlighting()
                        ? ['-ImportanceOrdering']
                        : ['-HighlightHighlighted', '!ImportanceOrdering', '-ImportanceOrdering'];

                    if (vm.lookupMode) {
                        order.push(function (attribute) {
                            return attribute.Value == 'Yes' ? -1 : 1;
                        });
                    }

                    order = order.concat(['-Count', '-Value']);
                }

                // if (category == 'Product Spend Profile') {
                //     console.log(category, order, subcategory, attr);
                // }

                return order;
            }

            vm.categoryClick = function (category, $event) {
                var target = angular.element($event.target),
                    currentTarget = angular.element($event.currentTarget);
                if (target.closest("[ng-click]:not(.ignore-ngclick)")[0] !== currentTarget[0]) {
                    // do nothing, user is clicking something with it's own click event
                } else {
                    var category = category || '';
                    if (vm.subcategory && vm.category == category) {
                        vm.setSubcategory('');
                        if (vm.subcategoriesExclude.indexOf(category) >= 0) { // don't show subcategories
                            vm.setSubcategory(vm.subcategories[category][0]);
                        }
                    } else if (vm.category == category) {
                        vm.setSubcategory('');
                        //vm.category = '';
                    } else {
                        vm.setSubcategory('');
                        if (vm.subcategoriesExclude.indexOf(category)) {
                            vm.setSubcategory(vm.subcategories[category][0]);
                        }
                        vm.setCategory(category);
                        vm.filterEmptySubcategories();
                    }
                    vm.metadata.current = 1;
                    vm.updateStateParams();
                }
            }

            vm.getAttributeStat = function (attribute) {
                var enrichmentKey = attribute.Attribute || attribute.ColumnId,
                    index = vm.enrichmentsMap[attribute.Entity + '.' + enrichmentKey],
                    enrichment = vm.enrichments[index],
                    attributeEntity = attribute.Entity,
                    stats = (vm.cube.data[attributeEntity].Stats[enrichmentKey] && vm.cube.data[attributeEntity].Stats[enrichmentKey] && vm.cube.data[attributeEntity].Stats[enrichmentKey].Bkts && vm.cube.data[attributeEntity].Stats[enrichmentKey].Bkts.List ? vm.cube.data[attributeEntity].Stats[enrichmentKey].Bkts.List : null),
                    segmentRangeKey = null;

                var stat = (stats && stats.length ? stats[0] : null);

                if (stat && stat.Rng) {
                    segmentRangeKey = vm.makeSegmentsRangeKey(enrichment, stat.Rng);
                }

                if (stats && stats.length > 1) {
                    for (var i in stats) {
                        if (stats[i] && stats[i].Rng) {
                            if (vm.segmentAttributeInputRange[vm.makeSegmentsRangeKey(enrichment, stats[i].Rng)]) {
                                stat = stats[i];
                                break;
                            }
                        }

                    }
                }

                return stat;
            }

            vm.getAttributeRange = function (attribute) {
                var stat = vm.getAttributeStat(attribute),
                    range = (stat && stat.Rng ? stat.Rng : {});
                return range;
            }

            vm.displayAttributeValue = function (attribute, property) {
                var property = property || 'Lbl',
                    enrichmentKey = attribute.Attribute || attribute.ColumnId,
                    attributeEntity = attribute.Entity,
                    stats = (vm.cube.data[attributeEntity].Stats[enrichmentKey] && vm.cube.data[attributeEntity].Stats[enrichmentKey] && vm.cube.data[attributeEntity].Stats[enrichmentKey].Bkts && vm.cube.data[attributeEntity].Stats[enrichmentKey].Bkts.List ? vm.cube.data[attributeEntity].Stats[enrichmentKey].Bkts.List : null);

                /**
                 * sort stats by record count if there are more then 1
                 */
                if (stats && stats.length > 1) {
                    stats = _.sortBy(stats, function (item) {
                        return parseInt(item.Cnt);
                    });
                }

                var stat = vm.getAttributeStat(attribute);

                if (stat && stat[property]) {
                    if (property === 'Lift') {
                        return stat[property].toFixed(1) + 'x';
                    }
                    return stat[property];
                }
            }

            vm.generateBucketOperation = function (bkt) {
                var ret = bkt.Cmp == 'NOT_EQUAL' ? 'is not' : 'is';

                return ret;
            }

            vm.generateBucketLabel = function (bkt) {
                var bkt = QueryStore.generateBucketLabel(bkt);

                return bkt.Lbl || 'empty';
            }

            vm.getTitleTooltip = function (attribute) {
                if (attribute.Entity === 'PurchaseHistory') {
                    return attribute.Subcategory;
                }
            }

            vm.getDateMap = function (category) {
                var categoryKey = category, //.toUpperCase().replace(' ','_'),
                    timestamp = (vm.collectionStatus && vm.collectionStatus.DateMap ? vm.collectionStatus.DateMap[categoryKey] : ''),
                    lastDataRefresh = '';
                if (timestamp) {
                    lastDataRefresh = 'Last Data Refresh: ' + moment(timestamp).format('MMMM DD, YYYY');
                    return lastDataRefresh;
                }
                return lastDataRefresh;
            }
        }
    };
};