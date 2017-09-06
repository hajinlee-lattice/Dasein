angular
.module('common.datacloud.query.advanced.tree', [])
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
        controller: function ($scope, DataCloudStore, QueryStore) {
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
                unused: false
            });

            vm.init = function (type, value) {
                vm.tree.collapsed = false;

                DataCloudStore.getEnrichments().then(function(enrichments) {
                    vm.enrichments = enrichments;
                    
                    if (vm.tree.bucketRestriction) {
                        var bucket = vm.tree.bucketRestriction;
                        
                        vm.item = vm.enrichments[ vm.enrichmentsMap[ bucket.attr.split('.')[1] ] ]
                        vm.root.pushItem(vm.item, vm.tree.bucketRestriction);
                        vm.type = vm.item.cube.Bkts.Type;
                        vm.label = vm.item.topbkt.Lbl;
                        vm.range = vm.item.topbkt.Rng;
                        
                        vm.setOperation(vm.item, vm.type, vm.label, vm.range);

                        if (typeof vm.tree.bucketRestriction.bkt.Id != "number") {
                            vm.tree.bucketRestriction.bkt.Id = vm.tree.labelGlyph;
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

            vm.setBucket = function($event) {
                if (vm.editing) {
                    vm.editing = false;
                    vm.unused = false;
                    
                    vm.root.updateCount();
                    vm.updateBucketCount();

                    $event.preventDefault();
                    $event.stopPropagation();
                }
            }
            
            vm.editBucket = function() {
                if (!vm.editing && (vm.type == 'Boolean' || vm.type == 'Numerical')) {
                    this.root.saveState(true);
                    vm.editing = true;
                }
            }

            vm.updateBucketCount = function() {
                vm.tree.bucketRestriction.bkt.Cnt = -1;

                vm.root.updateBucketCount(vm.tree.bucketRestriction).then(function(data) {
                    if (typeof data == 'number') {
                        vm.tree.bucketRestriction.bkt.Cnt = data;
                    }
                });
            }

            vm.addAttribute = function(tree) {
                this.root.saveState();
                QueryStore.setAddBucketTreeRoot(vm.tree);
                this.root.goAttributes();
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

            this.clickOperator = function($element) {
                this.root.saveState();
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
            }

            vm.init();
        }
    };
});