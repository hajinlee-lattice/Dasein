angular.module('common.datacloud.query.advanced', [
    'common.datacloud.query.advanced.input',
    'common.datacloud.query.advanced.tree'
])
.controller('AdvancedQueryCtrl', function(
    $state, $stateParams, $timeout, $q, QueryStore, $scope,
    QueryService, SegmentStore, DataCloudStore, Cube
) {
    var vm = this;

    angular.extend(this, {
        inModel: $state.current.name.split('.')[1] === 'model',
        mode: $stateParams.mode,
        cube: Cube,
        history: QueryStore.history,
        restriction: QueryStore.accountRestriction,
        enrichmentsMap: DataCloudStore.getEnrichmentsMap(),
        droppedItem: null,
        draggedItem: null,
        items: [],
        enrichments: [],
        labelIncrementor: 0,
        buckets: [
            { label: 'A', resource: 'accounts', count: 0, percentage: 0, active: true },
            { label: 'A-', resource: 'accounts', count: 0, percentage: 0, active: false },
            { label: 'B', resource: 'accounts', count: 0, percentage: 0, active: false },
            { label: 'C', resource: 'accounts', count: 0, percentage: 0, active: false },
            { label: 'D', resource: 'accounts', count: 0, percentage: 0, active: false },
            { label: 'F', resource: 'accounts', count: 0, percentage: 0, active: false }
        ]
    });

    vm.init = function() {
        
        DataCloudStore.getEnrichments().then(function(enrichments) {
            for (var i=0, enrichment; i<enrichments.length; i++) {
                enrichment = enrichments[i];

                if (!enrichment) {
                    continue;
                }

                vm.enrichmentsMap[enrichment.ColumnId] = i;
            }

            vm.enrichments = enrichments;

            DataCloudStore.setEnrichmentsMap(vm.enrichmentsMap);

            $timeout(function() {
                vm.tree = vm.getTree();
                vm.setCurrentSavedTree();
                console.log('[AQB] restriction:', angular.copy(vm.restriction));
                console.log('[AQB] items:', vm.items);
                console.log('[AQB] cube:', vm.cube);
            }, 1);
        });
    }

    vm.getTree = function() {
        //vm.generateRulesTree();
        switch (vm.mode) {
            case 'segment':
                return [ vm.restriction.restriction ];
            case 'rules':
                return [ vm.generateRulesTree().restriction ];
        }
    }

    vm.generateRulesTree = function() {
        var items = [
            'TechIndicator_AbsorbLMS',
            'LE_EMPLOYEE_RANGE',
            'TechIndicator_Adify',
            'LE_NUMBER_OF_LOCATIONS',
            'AlexaReachPerMillion',
        ];

        var items = DataCloudStore.getRatingEngineAttributes();
        var bucketRestrictions = [];
        items.forEach(function(value, index) {
            var item = vm.enrichments[vm.enrichmentsMap[value]]

            if (item) {
                bucketRestrictions.push({
                    bucketRestriction: {
                        attr: item.Entity + '.' + value,
                        bkt: {}
                    }
                })
            }
        });

        return {
            restriction: {
                logicalRestriction: {
                    operator: "AND",
                    restrictions: bucketRestrictions
                }
            }
        };
    }

    vm.pushItem = function(item, tree) {
        if (item) {
            var cube = vm.cube[item.ColumnId];

            item.cube = cube;
            item.topbkt = tree.bkt;

            vm.items.push(item);
        }
    }

    vm.setCurrentSavedTree = function() {
        console.log('SAVE CURRENT TREE', angular.copy(vm.tree))
        QueryStore.currentSavedTree = angular.copy(vm.tree);
    }

    vm.getBucketLabel = function(bucket) {
        if (bucket && bucket.labelGlyph) {
            return bucket.labelGlyph;
        } else {
            vm.labelIncrementor += 1;

            bucket.labelGlyph = vm.labelIncrementor;
            
            return vm.labelIncrementor;
        }
    }

    vm.saveState = function(noCount) {
        vm.labelIncrementor = 0;

        var current = angular.copy(vm.tree),
            old = angular.copy(vm.history[vm.history.length -1]) || [];

        console.log('saveState', vm.compareTree(old, current), current, old);

        if (!vm.compareTree(old, current)) {
            vm.history.push(current);

            if (!noCount) {
                vm.updateCount();
            }
        }
    }

    vm.clickUndo = function() {
        var lastState;

        while (lastState = vm.history.pop()) {
            console.log('clickUndo', vm.tree, lastState);
            
            if (vm.setState(lastState)) {
                vm.updateCount();

                break;
            }
        }
    }

    vm.setState = function(newState) {
        if (!vm.compareTree(newState, angular.copy(vm.tree))) {
            vm.labelIncrementor = 0;

            vm.restriction = {
                restriction: newState[0]
            };

            vm.tree = newState;

            return true;
        }

        return false;
    }

    vm.updateCount = function() {
        QueryStore.counts.accounts.loading = true;
        vm.prevBucketCountAttr = null;

        QueryService.GetCountByQuery('accounts', SegmentStore.sanitizeSegment({ 
            'free_form_text_search': "",
            'account_restriction': angular.copy(vm.restriction),
            'page_filter': {
                'num_rows': 20,
                'row_offset': 0
            }
        })).then(function(result) {
            QueryStore.setResourceTypeCount('accounts', false, result);
        });
    }

    vm.updateBucketCount = function(bucketRestriction) {
        var deferred = $q.defer();

        QueryService.GetCountByQuery('accounts', {
            "free_form_text_search": "",
            "account_restriction": {
                "restriction": {
                    "bucketRestriction": bucketRestriction
                }
            }
        }, bucketRestriction.attr == vm.prevBucketCountAttr).then(function(result) {
            deferred.resolve(result);
        });
        
        vm.prevBucketCountAttr = bucketRestriction.attr;

        return deferred.promise;
    }

    vm.saveSegment = function() {
        var segment = QueryStore.getSegment(),
            restriction = QueryStore.getAccountRestriction();

        vm.labelIncrementor = 0;
        vm.saving = true;

        SegmentStore.CreateOrUpdateSegment(segment, restriction).then(function(result) {
            vm.labelIncrementor = 0;
            vm.saving = false;
            vm.updateCount();
            vm.setCurrentSavedTree();
        });
    }

    vm.compareTree = function(old, current) {
        // remove AQB properties like labelGlyph/collapse
        SegmentStore.sanitizeSegmentRestriction(old);
        SegmentStore.sanitizeSegmentRestriction(current);

        return (JSON.stringify(old) === JSON.stringify(current));
    }

    vm.checkDisableSave = function() {
        if (!QueryStore.currentSavedTree || !vm.tree) {
            return true;
        }

        var old = angular.copy(QueryStore.currentSavedTree),
            current = angular.copy(vm.tree);

        return vm.compareTree(old, current);
    }

    vm.goAttributes = function() {
        var state = vm.inModel
                ? 'home.model.analysis.explorer.attributes'
                : 'home.segment.explorer.attributes';

        $state.go(state);
    }

    function swap(context, i, j) {
        var temp = context[i];
        context[i] = context[j]
        context[j] = temp;
    }

    vm.dropMoveItem = function(dragged, dropped, endMove) {
        console.log('dropMoveItem', dragged.parent !== dropped.parent, dragged, dropped);
        var items = dropped.parent 
                ? dropped.parent.logicalRestriction.restrictions
                : dropped.tree.logicalRestriction.restrictions;

        var theitem = null;
        var index = null;

        // items.forEach(function(item, i) {
        //     var conditional = item.$$hashKey == dropped.tree.$$hashKey;

        //     if (conditional) {
        //         theitem = item;
        //         index = i;
        //         return;
        //     }
            
        // });

        if (dropped.tree.logicalRestriction || dropped.parent.logicalRestriction) {
            var draggedParent = dragged.parent.logicalRestriction.restrictions;
            var droppedParent = dropped.parent 
                ? dropped.parent.logicalRestriction.restrictions 
                : [];

            var draggedIndex = draggedParent.indexOf(dragged.tree);
            var droppedIndex = droppedParent.indexOf(dropped.tree);
            
            var draggedItem = angular.copy(dragged.tree);

            console.log(draggedIndex, droppedIndex);

            if (dropped.tree.logicalRestriction) {
                dropped.tree.logicalRestriction.restrictions.splice(droppedIndex+1, 0, draggedItem);
            } else {
                droppedParent.splice(droppedIndex+1, 0, draggedItem);
            }

            draggedParent.splice(draggedParent.indexOf(dragged.tree), 1);
        }

        // if (typeof index === 'number') {
        //     //console.log(index, dragged);
        //     //console.log('dropMoveItem', items, items.length, index, index + 1)
        // }
    }

    vm.dropItem = function(branch) {
        console.log('dropped on', branch)
    }

    vm.init();
});