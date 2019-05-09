angular
    .module('common.datacloud.query.builder.tree', [
        'common.datacloud.query.builder.tree.service',
        'common.datacloud.query.builder.tree.info',
        'common.datacloud.query.builder.tree.edit',
        'common.datacloud.query.builder.tree.edit.transaction',
        'common.datacloud.query.builder.tree.edit.transaction.edit',
        'common.datacloud.query.builder.tree.edit.percent',
        'common.datacloud.query.builder.tree.edit.percent.edit',
        'common.datacloud.query.builder.tree.edit.percent.item',
        'common.datacloud.query.builder.tree.edit.date.attribute'
    ])
    .directive('queryTreeDirective', function () {
        return {
            restrict: 'AE',
            scope: {
                root: '=',
                tree: '=',
                parent: '=',
                entity: '='
            },
            templateUrl: '/components/datacloud/query/advanced/tree/tree.component.html',
            controllerAs: 'vm',
            controller: function ($scope, $timeout, $filter, DataCloudStore, QueryStore, QueryTreeService, RatingsEngineStore) {
                var vm = this;

                angular.extend(vm, {
                    root: $scope.root,
                    tree: $scope.tree,
                    parent: $scope.parent,
                    items: $scope.items,
                    entity: $scope.entity,
                    enrichments: [],
                    enrichmentsMap: DataCloudStore.getEnrichmentsMap(),
                    type: '',
                    label: '',
                    range: [],
                    operation: '',
                    unused: false,
                    uniqueId: Math.random() * (8 << 8),
                    editMode: 'Custom',
                    records_updating: false,
                    numerical_operations: QueryTreeService.numerical_operations,
                    enum_operations: QueryTreeService.enum_operations,
                    no_inputs: QueryTreeService.no_inputs,
                    two_inputs: QueryTreeService.two_inputs
                });

                vm.init = function (type, value) {
                    vm.tree.collapsed = false;

                    DataCloudStore.getEnrichments().then(function (enrichments) {
                        vm.enrichments = enrichments;
                        if (vm.tree.bucketRestriction) {
                            // console.log('BUCKET ',vm.tree.bucketRestriction.ignored);
                            if (vm.tree.bucketRestriction.ignored === undefined) {
                                vm.tree.bucketRestriction.ignored = false;
                            }

                            var bucket = vm.tree.bucketRestriction,
                                bucketEntity = bucket.attr.split('.')[0],
                                bucketColumnId = bucket.attr.split('.')[1];

                            vm.item = $filter('filter')(vm.enrichments, { Entity: bucketEntity, ColumnId: bucketColumnId }, true)[0];
                            // console.log('ITEM ', vm.item);
                            if (!vm.item || bucket.ignored === true) {
                                vm.unused = true;
                            } else {
                                vm.unused = bucket.ignored;
                            }
                            // console.log('IGNORE ', vm.unused);
                            if (vm.item) {
                                vm.root.pushItem(vm.item, vm.tree.bucketRestriction, vm);
                                if (vm.item.cube && vm.item.cube.Bkts) {
                                    vm.type = vm.item.cube.Bkts.Type;
                                    // console.log("Type =============> ",vm.type);
                                } else {
                                    //FIXME: if there is no Bkts, it is most likely a non-bucketable text field (YSong, Jan-2018)
                                    vm.type = 'String';
                                }
                            }

                            vm.label = vm.tree.bucketRestriction.bkt ? vm.tree.bucketRestriction.bkt.Lbl : '';
                            vm.range = QueryTreeService.getBktVals(vm.tree.bucketRestriction, vm.type)//vm.tree.bucketRestriction.bkt.Vals;
                        }
                    });
                }

                vm.isBucketUsed = function (bucket) {
                    var ret = QueryTreeService.isBucketUsed(bucket);//typeof bucket.bkt.Id == "number" && bucket.bkt.Vals && bucket.bkt.Vals.length > 0;
                    return ret;
                }

                vm.checkSelected = function (bucket) {
                    // console.log('checkSelected', bucket);
                    // Remove this method and remove vm.range/vm.label since they should not be necessary
                    if (bucket.Vals && bucket.Vals[0] == vm.range[0] && bucket.Vals[1] == vm.range[1]) {
                        vm.presetOperation = bucket.Lbl;
                    }
                }

                vm.changePreset = function (bucket) {
                    var label = vm.presetOperation;
                    var buckets = vm.item.cube.Bkts.List;
                    // var bucket = buckets.filter(function(item) { return item.Lbl == label; })[0];
                    if (bucket == undefined)
                        bucket = buckets.filter(function (item) { return item.Lbl == label; })[0];
                    var restriction = vm.tree.bucketRestriction.bkt;
                    var bkt = angular.copy(bucket);

                    restriction.Cmp = bkt.Cmp;
                    restriction.Id = bkt.Id;
                    restriction.Cnt = bkt.Cnt;
                    restriction.Lbl = bkt.Lbl;
                    if (bkt.Vals[0] !== undefined) {
                        if (restriction.Vals[0]) {
                            restriction.Vals[0] = bkt.Vals[0];
                        } else {
                            restriction.Vals = [bkt.Vals[0]];
                        }
                    } else if (restriction.Vals[0]) {
                        restriction.Vals.length = 0;
                    }


                    if (bkt.Vals[1] !== undefined) {
                        if (restriction.Vals[1]) {
                            restriction.Vals[1] = bkt.Vals[1];
                        } else {
                            restriction.Vals.push(bkt.Vals[1]);
                        }
                    } else if (restriction.Vals[1]) {
                        restriction.Vals.splice(1, 1);
                    }
                }

                vm.setBucket = function ($event, unset) {
                    vm.editing = false;

                    if (unset) {
                        vm.unused = true;
                        vm.tree.bucketRestriction.bkt = {};
                        vm.tree.bucketRestriction.ignored = true;
                    } else {
                        vm.unused = false;
                        vm.tree.bucketRestriction.ignored = false;
                    }

                    vm.records_updating = true;

                    vm.root.updateCount();
                    vm.updateBucketCount();

                    $event.preventDefault();
                    $event.stopPropagation();

                    $timeout(function () {
                        vm.records_updating = false;
                    }, 250);
                    console.log('[tree] setBucket()', vm.tree.bucketRestriction.bkt.Vals, vm.tree.bucketRestriction)
                }

                vm.editBucket = function () {
                    if (vm.root.draggedItem == vm || QueryStore.getPublicProperty('disableAllTreeRestrictions')) {
                        return;
                    }

                    if (!vm.editing && !vm.root.draggedItem &&
                        (vm.type == 'Date' || vm.type == 'Boolean' || vm.type == 'Numerical' || vm.type == 'Enum' || vm.type == 'TimeSeries' || vm.type == 'String' || vm.type == 'PercentChange')) {
                        if (vm.unused) {
                            vm.unused = false;
                            vm.tree.bucketRestriction.ignored = false;

                            if (vm.type != 'String') {
                                vm.item.topbkt = angular.copy(vm.item.cube.Bkts.List[0]);
                                vm.tree.bucketRestriction.bkt = angular.copy(vm.item.cube.Bkts.List[0]);
                            } else {
                                vm.item.topbkt = {
                                    "Lbl": " ",
                                    "Cmp": "IS_NOT_NULL",
                                    "Id": -1,
                                    "Cnt": vm.item.cube.Cnt,
                                    "Vals": [
                                        ""
                                    ]
                                };

                                vm.tree.bucketRestriction.bkt = {
                                    "Lbl": " ",
                                    "Cmp": "IS_NOT_NULL",
                                    "Id": -1,
                                    "Cnt": vm.item.cube.Cnt,
                                    "Vals": [
                                        ""
                                    ]
                                };
                            }

                            vm.label = vm.tree.bucketRestriction.bkt.Lbl;
                            vm.range = vm.tree.bucketRestriction.bkt.Vals;
                        }

                        vm.root.saveState(true);
                        vm.editing = true;
                    }
                }

                vm.updateBucketCount = function (segmentName) {
                    if (vm.root.mode != 'rules' || vm.root.mode != 'dashboardrules') {
                        vm.records_updating = true;

                        QueryTreeService.updateBucketCount(angular.copy(vm.tree.bucketRestriction), segmentName).then(function (data) {
                            if (typeof data == 'number') {
                                vm.tree.bucketRestriction.bkt.Cnt = data;
                            }

                            vm.records_updating = false;
                        });
                    }
                }

                // vm.changeBooleanValue = function() {
                //     if (!vm.tree.bucketRestriction.bkt.Vals[0]) {
                //         vm.tree.bucketRestriction.bkt.Vals[0] = null;
                //     }

                //     vm.updateBucketCount();
                // }

                vm.addAttribute = function (tree) {
                    this.root.saveState();
                    QueryStore.setAddBucketTreeRoot(vm.tree, vm.entity.toLowerCase());
                    this.root.goAttributes();
                }

                vm.mouseDown = function (event) {
                    if (vm.editing) {
                        return false;
                    }

                    vm.root.draggedItem = null;

                    vm.root.mouseDownTimer = $timeout(function () {
                        vm.root.draggedItem = vm;
                        vm.root.mouseDownTimer = false;
                        vm.mouseMove(event);
                    }, 150);
                }

                vm.mouseMove = function (event, dashedItem, append) {
                    var dragged = vm.root.draggedItem,
                        dropped = vm.root.droppedItem;

                    if (dragged) {
                        var rect = event.currentTarget.getBoundingClientRect(),
                            offsetY = event.clientY - rect.top;

                        if (!dashedItem) {
                            vm.root.droppedItemAppend = (offsetY / rect.height) >= 0.5;
                        } else if (append) {
                            vm.root.droppedItemAppend = append || false;
                        }

                        if (!vm.root.draggedClone || !vm.root.draggedContainer) {
                            vm.root.draggedContainer = angular.element('.advanced-query-builder');

                            vm.root.draggedClone = angular.element(event.currentTarget.parentNode.cloneNode(false));
                            vm.root.draggedClone.append(event.currentTarget.cloneNode(true));

                            vm.root.draggedContainer.append(vm.root.draggedClone);
                            vm.root.draggedClone.addClass('query-section').addClass('dragging');
                        }

                        vm.rect = vm.root.draggedContainer[0].getBoundingClientRect();

                        var x = event.clientX - vm.rect.left + 10;
                        var y = event.clientY - vm.rect.top - 51;
                        var t = 'translate(' + x + 'px,' + y + 'px) scale(0.8, 0.8)';

                        vm.root.draggedClone.css({
                            '-webkit-transform': t,
                            '-moz-transform': t,
                            '-ms-transform': t,
                            'transform': t,
                        });
                    }
                }

                vm.mouseOver = function (event) {
                    var dragged = vm.root.draggedItem,
                        dropped = vm.root.droppedItem;

                    if (dragged && (!dropped || (dropped && dropped.tree.$$hashKey !== vm.tree.$$hashKey))) {
                        vm.root.droppedItem = vm;
                    }
                }

                vm.addOperator = function (tree) {
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

                vm.clickOperator = function () {
                    $timeout(function () {
                        vm.root.saveState();
                    }, 50);
                }

                // vm.clickEditMode = function(value) {
                //     vm.editMode = value;
                //     if(value !== 'Custom'){
                //         console.log('Preset');
                //         var bucket = vm.getCubeBktList()[0]
                //         vm.changePreset(bucket);
                //     }else{
                //         QueryTreeService.resetBktValues();
                //     }
                // }

                vm.clickCollapsed = function () {
                    // FIXME - collapsed property is weeded out of equivalency check
                    //vm.root.saveState(true); // true wont update counts

                    vm.tree.collapsed = !vm.tree.collapsed;
                }

                vm.clickDelete = function () {

                    vm.root.saveState();

                    vm.parent.logicalRestriction.restrictions.forEach(function (item, index) {
                        if (item == vm.tree) {
                            if (vm.parent.bucketRestriction || vm.parent.logicalRestriction) {
                                vm.parent.logicalRestriction.restrictions.splice(index, 1);
                            }
                        }
                    });
                    vm.root.updateCount();
                }

                vm.buckRestrictionSortBy = function () {
                    return function (object) {
                        return object.bucketRestriction && object.bucketRestriction.bkt && object.bucketRestriction.bkt.Id;
                    }
                }

                vm.categoryClass = function (category) {
                    return category ? category.toLowerCase().replace(/\s/g, "_") : '';
                }

                vm.disableAllRestrictions = function () {
                    return QueryStore.getPublicProperty('disableAllTreeRestrictions');
                }

                vm.init();
            }
        };
    });