angular
.module('common.datacloud.query.builder.tree', [])
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
        controller: function ($scope, $timeout, DataCloudStore, QueryStore) {
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
                mouseDownTimer: false
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

                        vm.root.pushItem(vm.item, vm.tree.bucketRestriction);
                        vm.type = vm.item.cube.Bkts.Type;
                        vm.label = vm.item.topbkt.Lbl;
                        vm.range = vm.item.topbkt.Rng;
                        
                        vm.setOperation(vm.item, vm.type, vm.label, vm.range);

                        if (typeof vm.tree.bucketRestriction.bkt.Id != "number") {
                            //vm.tree.bucketRestriction.bkt.Id = vm.tree.labelGlyph;
                            vm.unused = true;
                        }
                    }
                });
            }

            vm.setOperation = function(item, type, label, range) {
                if (type == 'Numerical') {
                    if (range) {
                        if (range[0] != null && range[1] != null && range[0] === range[1]) {
                            vm.operation = 'is';
                        } else if (range[0] != null && range[1] != null) {
                            vm.operation = 'between';
                        } else if (range[0] == null) {
                            vm.operation = 'less';
                        } else if (range[1] == null) {
                            vm.operation = 'greater_equal';
                        } else {
                            vm.operation = 'empty';
                        }
                    } else {
                        vm.operation = 'empty';
                    }
                }
            }

            vm.checkSelected = function(bucket) {
                console.log(bucket);
                if (bucket.Rng && bucket.Rng[0] == vm.range[0] && bucket.Rng[1] == vm.range[1]) {
                    //console.log('selected', (bucket.Rng[0] == vm.range[0] && bucket.Rng[1] == vm.range[1]), bucket.Rng[0], vm.range[0], bucket.Rng[1], vm.range[1], bucket, vm.range);
                    vm.presetOperation = bucket.Lbl;
                }
            }

            vm.changePreset = function() {
                var label = vm.presetOperation;
                var buckets = vm.item.cube.Bkts.List;
                var bucket = buckets.filter(function(item) { return item.Lbl == label; })[0];
                var restriction = vm.tree.bucketRestriction.bkt;
                var bkt = angular.copy(bucket);

                restriction.Cnt = bkt.Cnt;
                restriction.Id = bkt.Id;
                restriction.Lbl = bkt.Lbl;
                restriction.Rng[0] = bkt.Rng[0];
                restriction.Rng[1] = bkt.Rng[1];

                vm.setOperation(null, 'Numerical', null, bkt.Rng) 
                //console.log(label, bucket, buckets, restriction);
            }

            vm.changeOperation = function() {
                if (!vm.item.topbkt.Rng) {
                    vm.item.topbkt.Rng = [null, null];
                }

                switch (vm.operation) {
                    case 'is':
                        vm.item.topbkt.Rng = [ vm.item.topbkt.Rng[0], vm.item.topbkt.Rng[0] ];
                        vm.item.topbkt.Lbl = vm.item.topbkt.Rng[0];
                        break;
                    case 'between': 
                        vm.item.topbkt.Lbl = vm.item.topbkt.Rng[0] + ' - ' + vm.item.topbkt.Rng[1];
                        break;
                    case 'less': 
                        vm.item.topbkt.Rng[0] = null;
                        vm.item.topbkt.Lbl = '< ' + vm.item.topbkt.Rng[1];
                        break;
                    case 'greater_equal': 
                        vm.item.topbkt.Rng[1] = null;
                        vm.item.topbkt.Lbl = '>= ' + vm.item.topbkt.Rng[0];
                        break;
                    case 'empty': 
                        vm.item.topbkt.Rng = null;
                        vm.item.topbkt.Lbl = null;
                        break;
                }

                vm.updateBucketCount();
                vm.range = vm.item.topbkt.Rng;
            }

            vm.getOperationLabel = function(operation) {
                var map = {
                    "Yes": "is",
                    "No": "is not",
                    "": "is empty",
                    "is": "is",
                    "empty": "is empty",
                    "less": "is less than",
                    "less_equal": "is less than",
                    "greater": "is greater than",
                    "greater_equal": "is greater than",
                    "between": "is between"
                };

                switch (vm.type) {
                    case 'Boolean': return map[vm.item.topbkt.Lbl];
                    case 'Numerical': return map[vm.operation];
                    case 'Enum': return 'has a value of';
                    default: 
                        //console.log('unknown type', vm.type, vm.label, vm.operation, vm.item);
                        return 'has a value of';
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
                if (!vm.editing && !vm.root.draggedItem && (vm.type == 'Boolean' || vm.type == 'Numerical')) {
                    if (vm.unused) {
                        vm.unused = false;

                        vm.item.topbkt = angular.copy(vm.item.cube.Bkts.List[0]);
                        vm.tree.bucketRestriction.bkt = angular.copy(vm.item.cube.Bkts.List[0]);

                        vm.label = vm.item.topbkt.Lbl;
                        vm.range = vm.item.topbkt.Rng;

                        vm.setOperation(vm.item, vm.type, vm.label, vm.range);
                    }

                    vm.root.saveState(true);
                    vm.editing = true;
                }
            }

            vm.updateBucketCount = function() {
                if (vm.root.mode == 'rules') {
                    vm.root.getRuleRecordCounts([ vm.tree ]);
                } else {
                    vm.tree.bucketRestriction.bkt.Cnt = -1;

                    vm.root.updateBucketCount(vm.tree.bucketRestriction).then(function(data) {
                        if (typeof data == 'number') {
                            vm.tree.bucketRestriction.bkt.Cnt = data;
                        }
                    });
                }
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
                }, 333);
            }

            vm.mouseUp = function() {
                var dragged = vm.root.draggedItem,
                    dropped = vm.root.droppedItem;

                if (dragged && (!dropped || (dropped && dropped.uniqueId !== dragged.uniqueId))) {
                    //console.log('mouseUp', dragged.uniqueId, dropped.uniqueId, dragged, dropped);
                    vm.root.droppedItem = vm;
                    
                    if (dropped) {
                        this.root.saveState();
                        vm.root.dropMoveItem(dragged, dropped);
                    }
                }

                vm.root.draggedItem = null;
                vm.root.droppedItem = null;

                $timeout.cancel(vm.mouseDownTimer);
                vm.mouseDownTimer = false;
            }

            vm.mouseOver = function() {
                var dragged = vm.root.draggedItem,
                    dropped = vm.root.droppedItem;

                if (dragged && (!dropped || (dropped && dropped.tree.$$hashKey !== vm.tree.$$hashKey))) {
                    vm.root.droppedItem = vm;
                    
                    if (dropped) {
                        //vm.root.dropMoveItem(dragged, dropped);
                        //vm.root.draggedItem = vm;
                    }
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

            this.clickOperator = function() {
                this.root.saveState();
            }

            this.clickEditMode = function(value) {
                vm.editMode = value;
            }

            this.clickCollapsed = function() {
                // FIXME - collapsed property is weeded out of equivalency check
                //this.root.saveState(true); // true wont update counts

                vm.tree.collapsed = !vm.tree.collapsed;
            }

            this.clickDelete = function() {
                this.root.saveState();

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