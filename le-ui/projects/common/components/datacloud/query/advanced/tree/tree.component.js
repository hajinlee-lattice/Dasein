angular
.module('common.datacloud.query.builder.tree', [
    'common.datacloud.query.builder.tree.service', 
    'common.datacloud.query.builder.tree.info',
    'common.datacloud.query.builder.tree.edit'
])
.directive('queryTreeDirective',function() {
    return {
        restrict: 'AE',
        scope: {
            root: '=',
            tree: '=',
            parent: '='
        },
        templateUrl: '/components/datacloud/query/advanced/tree/tree.component.html',
        controllerAs: 'vm',
        controller: function ($scope, $timeout, DataCloudStore, QueryStore, QueryTreeService) {
            var vm = this;

            angular.extend(vm, {
                root: $scope.root,
                tree: $scope.tree,
                parent: $scope.parent,
                items: $scope.root.items,
                enrichments: [],
                enrichmentsMap: DataCloudStore.getEnrichmentsMap(),
                type: '',
                label: '',
                range: [],
                operation: '',
                unused: false,
                uniqueId: $scope.tree.$$hashKey,
                editMode: 'Custom',
                records_updating: false,
                mouseDownTimer: false,
                numerical_operations: QueryTreeService.numerical_operations,
                enum_operations: QueryTreeService.enum_operations,
                no_inputs: QueryTreeService.no_inputs,
                two_inputs: QueryTreeService.two_inputs
            });

            vm.init = function (type, value) {
                vm.tree.collapsed = false;

                DataCloudStore.getEnrichments().then(function(enrichments) {
                    vm.enrichments = enrichments;
                    
                    if (vm.tree.bucketRestriction) {
                        var bucket = vm.tree.bucketRestriction;
                        
                        vm.item = angular.copy(
                            vm.enrichments[
                                vm.enrichmentsMap[
                                    bucket.attr.split('.')[1]
                                ]
                            ]
                        );

                        if (!vm.item || typeof vm.tree.bucketRestriction.bkt.Id != "number") {
                            vm.unused = true;
                        }

                        if (vm.item) {
                            vm.root.pushItem(vm.item, vm.tree.bucketRestriction);
                            vm.type = vm.item.cube.Bkts.Type;
                        }

                        vm.label = vm.tree.bucketRestriction.bkt.Lbl;
                        vm.range = vm.tree.bucketRestriction.bkt.Vals;
                    }
                });
            }

            vm.checkSelected = function(bucket) {
                if (bucket.Vals && bucket.Vals[0] == vm.range[0] && bucket.Vals[1] == vm.range[1]) {
                    vm.presetOperation = bucket.Lbl;
                }
            }

            vm.changePreset = function() {
                var label = vm.presetOperation;
                var buckets = vm.item.cube.Bkts.List;
                var bucket = buckets.filter(function(item) { return item.Lbl == label; })[0];
                var restriction = vm.tree.bucketRestriction.bkt;
                var bkt = angular.copy(bucket);

                restriction.Cmp = bkt.Cmp;
                restriction.Id = bkt.Id;
                restriction.Cnt = bkt.Cnt;
                restriction.Lbl = bkt.Lbl;

                if (bkt.Vals[0]) {
                    if (restriction.Vals[0]) {
                        restriction.Vals[0] = bkt.Vals[0];
                    } else {
                        restriction.Vals = [ bkt.Vals[0] ];
                    }
                } else if (restriction.Vals[0]) {
                    restriction.Vals.length = 0;
                }


                if (bkt.Vals[1]) {
                    if (restriction.Vals[1]) {
                        restriction.Vals[1] = bkt.Vals[1];
                    } else {
                        restriction.Vals.push(bkt.Vals[1])
                    }
                } else if (restriction.Vals[1]) {
                    restriction.Vals.splice(1,1);
                }
            }

            vm.getOperationLabel = function() {
                if (!vm.tree.bucketRestriction.bkt) {
                    return;
                }

                switch (vm.type) {
                    case 'Boolean': return QueryTreeService.cmpMap[vm.tree.bucketRestriction.bkt.Vals[0] || ''];
                    case 'Numerical': return QueryTreeService.cmpMap[vm.tree.bucketRestriction.bkt.Cmp];
                    case 'Enum': return QueryTreeService.cmpMap[vm.tree.bucketRestriction.bkt.Cmp];
                    default: return 'has a value of';
                }
            }

            vm.setBucket = function($event, unset) {
                vm.editing = false;
                
                if (unset) {
                    vm.unused = true;
                    vm.tree.bucketRestriction.bkt = {};
                } else {
                    vm.unused = false;
                }

                vm.root.updateCount();
                vm.updateBucketCount();

                $event.preventDefault();
                $event.stopPropagation();
            }
            
            vm.editBucket = function() {
                if (vm.root.draggedItem == vm) {
                    return;
                }

                if (!vm.editing && !vm.root.draggedItem && (vm.type == 'Boolean' || vm.type == 'Numerical' || vm.type == 'Enum')) {
                    if (vm.unused) {
                        vm.unused = false;

                        vm.item.topbkt = angular.copy(vm.item.cube.Bkts.List[0]);
                        vm.tree.bucketRestriction.bkt = angular.copy(vm.item.cube.Bkts.List[0]);

                        vm.label = vm.tree.bucketRestriction.bkt.Lbl;
                        vm.range = vm.tree.bucketRestriction.bkt.Vals;
                    }

                    vm.root.saveState(true);
                    vm.editing = true;
                }
            }

            vm.updateBucketCount = function() {
                if (vm.root.mode != 'rules') {
                    vm.records_updating = true;

                    vm.root.updateBucketCount(vm.tree.bucketRestriction).then(function(data) {
                        if (typeof data == 'number') {
                            vm.tree.bucketRestriction.bkt.Cnt = data;
                        }
                        
                        vm.records_updating = false;
                    });
                }
            }

            vm.changeBooleanValue = function() {
                if (!vm.tree.bucketRestriction.bkt.Vals[0]) {
                    vm.tree.bucketRestriction.bkt.Vals[0] = null;
                }

                vm.updateBucketCount();
            }

            vm.addAttribute = function(tree) {
                this.root.saveState();
                QueryStore.setAddBucketTreeRoot(vm.tree);
                this.root.goAttributes();
            }

            vm.mouseDown = function() {
                vm.root.draggedItem = null;

                vm.mouseDownTimer = $timeout(function() {
                    vm.root.draggedItem = vm;
                    vm.mouseDownTimer = false;
                }, 150);
            }

            vm.mouseUp = function() {
                var dragged = vm.root.draggedItem,
                    dropped = vm.root.droppedItem;

                if (dragged && (!dropped || (dropped && dropped.uniqueId !== dragged.uniqueId))) {
                    vm.root.droppedItem = vm;
                    
                    if (dropped) {
                        this.root.saveState();
                        vm.root.dropMoveItem(dragged, dropped);
                    }
                }

                $timeout(function() {
                    vm.root.draggedItem = null;
                    vm.root.droppedItem = null;
                }, 100)

                $timeout.cancel(vm.mouseDownTimer);
                vm.mouseDownTimer = false;
            }

            vm.mouseOver = function() {
                var dragged = vm.root.draggedItem,
                    dropped = vm.root.droppedItem;

                if (dragged && (!dropped || (dropped && dropped.tree.$$hashKey !== vm.tree.$$hashKey))) {
                    vm.root.droppedItem = vm;
                }
            }

            vm.addOperator = function(tree) {
                var operator = tree.logicalRestriction.operator == 'AND' ? 'OR' : 'AND';

                this.root.saveState();

                if (tree.logicalRestriction) {
                    tree.logicalRestriction.restrictions.push({
                        logicalRestriction: {
                            operator: operator,
                            restrictions: []
                        }
                    })
                }
            }

            vm.clickOperator = function() {
                $timeout(function() {
                    vm.root.saveState();
                }, 50);
            }

            vm.clickEditMode = function(value) {
                vm.editMode = value;
            }

            vm.clickCollapsed = function() {
                // FIXME - collapsed property is weeded out of equivalency check
                //vm.root.saveState(true); // true wont update counts

                vm.tree.collapsed = !vm.tree.collapsed;
            }

            vm.clickDelete = function() {
                vm.root.saveState();

                vm.parent.logicalRestriction.restrictions.forEach(function(item, index) {
                    if (item == vm.tree) {
                        if (vm.parent.bucketRestriction || vm.parent.logicalRestriction) {
                            vm.parent.logicalRestriction.restrictions.splice(index, 1);
                        }
                    }
                });

                vm.root.updateCount();
            }

            vm.buckRestrictionSortBy = function() {
                return function(object) {
                    return object.bucketRestriction && object.bucketRestriction.bkt && object.bucketRestriction.bkt.Id;
                }
            }

            vm.init();
        }
    };
});