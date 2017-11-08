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
                mouseDownTimer: false,
                numerical_operations: {
                    'IS_NULL': 'Empty',
                    'IS_NOT_NULL': 'Not Empty',
                    'EQUAL': 'Equal',
                    'NOT_EQUAL': 'Not Equal',
                    'GREATER_THAN': 'Greater Than',
                    'GREATER_OR_EQUAL': 'Greater or Equal',
                    'LESS_THAN': 'Less Than',
                    'LESS_OR_EQUAL': 'Lesser or Equal',
                    'GTE_AND_LTE': '>= and <=',
                    'GTE_AND_LT': '>= and <',
                    'GT_AND_LTE': "> and <=",
                    'GT_AND_LT': "> and <"
                },
                enum_operations: {
                    //'IS_NULL': 'Is Empty',
                    //'IS_NOT_NULL': 'Is Present',
                    'EQUAL': 'Is Equal To',
                    'NOT_EQUAL': 'Does Not Equal',
                    //'IN_COLLECTION': 'Is In Collection',
                    //'CONTAINS': 'String Contains',
                    //'NOT_CONTAINS': "Does Not Contains",
                    //'STARTS_WITH': 'String Starts With'
                },
                no_inputs: [
                    'IS_NULL',
                    'IS_NOT_NULL'
                ],
                two_inputs: [
                    'GTE_AND_LTE',
                    'GTE_AND_LT',
                    'GT_AND_LTE',
                    'GT_AND_LT'
                ]
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
                        
                        //vm.setOperation(vm.item, vm.type, vm.label, vm.range);
                    }
                });
            }

            vm.setOperation = function(item, type, label, range) {
                // if (type == 'Numerical') {
                //     if (range) {
                //         if (range[0] != null && range[1] != null && range[0] === range[1]) {
                //             vm.operation = 'is';
                //         } else if (range[0] != null && range[1] != null) {
                //             vm.operation = 'between';
                //         } else if (range[0] == null) {
                //             vm.operation = 'less';
                //         } else if (range[1] == null) {
                //             vm.operation = 'greater_equal';
                //         } else {
                //             vm.operation = 'empty';
                //         }
                //     } else {
                //         vm.operation = 'empty';
                //     }
                // }
            }

            vm.checkSelected = function(bucket) {
                // console.log(bucket);
                if (bucket.Vals && bucket.Vals[0] == vm.range[0] && bucket.Vals[1] == vm.range[1]) {
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

                //vm.setOperation(null, 'Numerical', null, bkt.Rng) 
            }

            vm.changeOperation = function() {
                // if (!vm.tree.bucketRestriction.bkt.Rng) {
                //     vm.tree.bucketRestriction.bkt.Rng = [null, null];
                // }

                // switch (vm.operation) {
                //     case 'is':
                //         vm.tree.bucketRestriction.bkt.Rng = [ vm.tree.bucketRestriction.bkt.Rng[0], vm.tree.bucketRestriction.bkt.Rng[0] ];
                //         vm.tree.bucketRestriction.bkt.Lbl = vm.tree.bucketRestriction.bkt.Rng[0];
                //         break;
                //     case 'between': 
                //         vm.tree.bucketRestriction.bkt.Lbl = vm.tree.bucketRestriction.bkt.Rng[0] + ' - ' + vm.tree.bucketRestriction.bkt.Rng[1];
                //         break;
                //     case 'less': 
                //         vm.tree.bucketRestriction.bkt.Rng[0] = null;
                //         vm.tree.bucketRestriction.bkt.Lbl = '< ' + vm.tree.bucketRestriction.bkt.Rng[1];
                //         break;
                //     case 'greater_equal': 
                //         vm.tree.bucketRestriction.bkt.Rng[1] = null;
                //         vm.tree.bucketRestriction.bkt.Lbl = '>= ' + vm.tree.bucketRestriction.bkt.Rng[0];
                //         break;
                //     case 'empty': 
                //         vm.tree.bucketRestriction.bkt.Rng = null;
                //         vm.tree.bucketRestriction.bkt.Lbl = null;
                //         break;
                // }

                // vm.updateBucketCount();
                // vm.range = vm.tree.bucketRestriction.bkt.Rng;
            }

            vm.getOperationLabel = function() {
                if (!vm.tree.bucketRestriction.bkt) {
                    return;
                }

                var map = {
                    "Yes": "is",
                    "No": "is not",
                    "": "is empty",
                    "is": "is",
                    "empty": "is empty",
                    "between": "is between",
                    'IS_NULL': 'is empty',
                    'IS_NOT_NULL': 'is present',
                    'EQUAL': 'is equal to',
                    'NOT_EQUAL': 'is not equal to',
                    'GREATER_THAN': 'is greater than',
                    'GREATER_OR_EQUAL': 'is greater than or equal to',
                    'LESS_THAN': 'is less than',
                    'LESS_OR_EQUAL': 'is less than or equal to',
                    'GTE_AND_LTE': 'is greater or equal and lesser or equal',
                    'GTE_AND_LT': 'is greater or equal and less than',
                    'GT_AND_LTE': "is greater than and lesser or equal",
                    'GT_AND_LT': "is greater than and less than",
                    'IN_COLLECTION': 'in collection',
                    'CONTAINS': 'contains',
                    'NOT_CONTAINS': 'not contains',
                    'STARTS_WITH': 'starts with'
                };

                switch (vm.type) {
                    case 'Boolean': return map[vm.tree.bucketRestriction.bkt.Vals[0] || ''];
                    case 'Numerical': return map[vm.tree.bucketRestriction.bkt.Cmp];
                    case 'Enum': return map[vm.tree.bucketRestriction.bkt.Cmp];
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

                        //console.log(vm.label, vm.type, vm.item.DisplayName, vm.item);
                        vm.item.topbkt = angular.copy(vm.item.cube.Bkts.List[0]);
                        vm.tree.bucketRestriction.bkt = angular.copy(vm.item.cube.Bkts.List[0]);

                        vm.label = vm.tree.bucketRestriction.bkt.Lbl;
                        vm.range = vm.tree.bucketRestriction.bkt.Vals;

                        //vm.setOperation(vm.item, vm.type, vm.label, vm.range);
                    }

                    vm.root.saveState(true);
                    vm.editing = true;
                }
            }

            vm.updateBucketCount = function() {
                //console.log('updateBucketCount', vm.item.DisplayName, vm.item, vm);
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
                    //vm.unused = true;
                }, 150);
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
                        //vm.unused = false;
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
                $timeout(function() {
                    this.root.saveState();
                }, 50);
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