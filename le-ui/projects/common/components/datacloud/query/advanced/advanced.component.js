angular.module('common.datacloud.query.builder', [
    'common.datacloud.query.builder.input',
    'common.datacloud.query.builder.tree'
])
.controller('AdvancedQueryCtrl', function(
    $state, $stateParams, $timeout, $q, QueryStore, $scope, QueryService, $element,
    SegmentStore, DataCloudStore, Cube, CurrentRatingsEngine, CoverageMap, RatingsEngineStore
) {
    var vm = this;

    angular.extend(this, {
        inModel: $state.current.name.split('.')[1] === 'model',
        mode: CurrentRatingsEngine !== null ? 'rules' : 'segment',
        cube: Cube,
        history: QueryStore.history,
        restriction: QueryStore.accountRestriction,
        enrichmentsMap: DataCloudStore.getEnrichmentsMap(),
        droppedItem: null,
        draggedItem: null,
        items: [],
        enrichments: [],
        labelIncrementor: 0,
        bucket: 'A',
        buckets: [
            { bucket: 'A',  count: 0 },
            { bucket: 'A-', count: 0 },
            { bucket: 'B',  count: 0 },
            { bucket: 'C',  count: 0 },
            { bucket: 'D',  count: 0 },
            { bucket: 'F',  count: 0 }
        ],
        bucketsMap: {'A':0,'A-':1,'B':2,'C':3,'D':4,'F':5},
        default_bucket: 'A',
        rating_rule: {},
        coverage_map: {},
        rating_id: $stateParams.rating_id,
        treeMode: 'account'
    });

    vm.init = function() {
        console.log('[AQB] CoverageMap:', CoverageMap);
        console.log('[AQB] CurrentRatingsEngine:', CurrentRatingsEngine);

        if (CurrentRatingsEngine) {
            vm.rating_rule = CurrentRatingsEngine.rule.ratingRule;
            vm.rating_buckets = vm.rating_rule.bucketToRuleMap;
            vm.default_bucket = vm.rating_rule.defaultBucketName;
        }

        if (CoverageMap) {
            vm.initCoverageMap(CoverageMap);
        }

        console.log('[AQB] rating_rule:', vm.rating_rule);

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

    vm.initCoverageMap = function(map) {
        var segmentId = Object.keys(map.segmentIdModelRulesCoverageMap)[0];

        vm.coverage_map = map.segmentIdModelRulesCoverageMap[segmentId];
        
        vm.coverage_map.bucketCoverageCounts.forEach(function(bkt) {
            vm.buckets[vm.bucketsMap[bkt.bucket]].count = bkt.count;
        });
    }

    vm.getTree = function() {
        switch (vm.mode) {
            case 'segment':
                return [ vm.restriction.restriction ];
            case 'rules':
                return [ vm.generateRulesTree() ];
        }
    }

    vm.generateRulesTree = function() {
        var items = CurrentRatingsEngine.rule.selectedAttributes;
        var bucketRestrictions = [];
        
        items.forEach(function(value, index) {
            var item = angular.copy(vm.enrichments[vm.enrichmentsMap[value]]);

            if (item) {
                bucketRestrictions.push({
                    bucketRestriction: {
                        attr: item.Entity + '.' + value,
                        bkt: {}
                    }
                })
            }
        });

        if (vm.bucket) {
            var bucket = vm.rating_rule.bucketToRuleMap[vm.bucket];
            var fromBucket = bucket[vm.treeMode + '_restriction'];
            var restrictions = fromBucket.logicalRestriction.restrictions;
            var ids = [];

            restrictions.forEach(function(value, index) {
                ids.push(value.bucketRestriction.attr);
            })

            bucketRestrictions.forEach(function(value, index) {
                if (ids.indexOf(value.bucketRestriction.attr) < 0) {
                    restrictions.push(value);
                }
            })

            console.log('generateRulesTree',vm.bucket, bucket, fromBucket, bucketRestrictions);
        }

        return fromBucket;
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

        //console.log('saveState', vm.compareTree(old, current), current, old);

        if (!vm.compareTree(old, current)) {
            vm.history.push(current);

            if (!noCount) {
                vm.updateCount();
            }
        }
    }
    
    vm.compare = function($element) {
        console.log('compare', vm.rating_rule.defaultBucketName == $element, vm.rating_rule.defaultBucketName, $element);
        return vm.rating_rule.defaultBucketName == $element;
    }

    vm.clickDefaultBucket = function($element) {
        //vm.rating_rule.defaultBucketName = bucket.bucket;
        console.log('clickDefaultBucket', vm.rating_rule.defaultBucketName, $element)
    }

    vm.clickBucketTile = function(bucket) {
        console.log('clickBucketTile', vm.rating_rule.defaultBucketName, bucket.bucket)
        vm.bucket = bucket.bucket;
        vm.tree = vm.getTree();
    }

    vm.getRuleCount = function(bkt) {
        if (bkt) {
            var buckets = [
                vm.rating_rule.bucketToRuleMap[bkt.bucket] 
            ];
        } else {
            var buckets = [ 
                vm.rating_rule.bucketToRuleMap['A'], 
                vm.rating_rule.bucketToRuleMap['A-'], 
                vm.rating_rule.bucketToRuleMap['B'], 
                vm.rating_rule.bucketToRuleMap['C'], 
                vm.rating_rule.bucketToRuleMap['D'], 
                vm.rating_rule.bucketToRuleMap['F'] 
            ];
        }

        var filtered = [];

        buckets.forEach(function(value, index) {
            var bucket = value;
            var restrictions = bucket.account_restriction.logicalRestriction.restrictions;
            
            filtered = filtered.concat(restrictions.filter(function(value, index) {
                return value.bucketRestriction.bkt.Id;
            }));
        })

        //console.log('getRuleCount', bkt, filtered);

        return filtered.length;
    }
    
    vm.clickUndo = function() {
        var lastState;

        while (lastState = vm.history.pop()) {
            //console.log('clickUndo', vm.tree, lastState);
            
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
        QueryStore.counts[vm.treeMode + 's'].loading = true;
        vm.prevBucketCountAttr = null;

        if (vm.mode == 'rules') {
            var RatingEngineCopy = angular.copy(CurrentRatingsEngine);
            var BucketMap = RatingEngineCopy.rule.ratingRule.bucketToRuleMap;
            var buckets = ['A','A-','B','C','D','F'];

            buckets.forEach(function(bucketName, index) {
                var bucket = BucketMap[bucketName];
                var restrictions = bucket[vm.treeMode + '_restriction'].logicalRestriction.restrictions;
                var pruned = [];

                pruned = restrictions.filter(function(restriction, index) {
                    var bRestriction = restriction.bucketRestriction;

                    return bRestriction && bRestriction.bkt.Id;
                });

                BucketMap[bucketName][vm.treeMode + '_restriction'].logicalRestriction.restrictions = pruned;

                SegmentStore.sanitizeSegmentRestriction([ BucketMap[bucketName][vm.treeMode + '_restriction'] ]);
            })

            console.log('updateCount rules', RatingEngineCopy);
            $timeout(function() {
                RatingsEngineStore.getCoverageMap(RatingEngineCopy).then(function(result) {
                    vm.initCoverageMap(result);
                }); 
            }, 100);
            
        } else {

            $timeout(function() {
                var segment = { 
                    'free_form_text_search': "",
                    'page_filter': {
                        'num_rows': 20,
                        'row_offset': 0
                    }
                };

                segment[vm.treeMode + '_restriction'] = angular.copy(vm.restriction),

                QueryService.GetCountByQuery(vm.treeMode + 's', SegmentStore.sanitizeSegment(segment)).then(function(result) {
                    QueryStore.setResourceTypeCount(vm.treeMode + 's', false, result);
                });
            }, 100);
            
        }
    }

    vm.updateBucketCount = function(bucketRestriction) {
        var deferred = $q.defer();

        var segment = {
            "free_form_text_search": ""
        };

        segment[vm.treeMode + '_restriction'] = {
            "restriction": {
                "bucketRestriction": bucketRestriction
            }
        };

        QueryService.GetCountByQuery(
            vm.treeMode + 's', 
            segment, 
            bucketRestriction.attr == vm.prevBucketCountAttr
        ).then(function(result) {
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
        return false;

        // FIXME: this stuff is disabled for now
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

    vm.dropMoveItem = function(dragged, dropped, endMove) {
        console.log('dropMoveItem', dragged.parent !== dropped.parent, dragged, dropped);
        var items = dropped.parent 
                ? dropped.parent.logicalRestriction.restrictions
                : dropped.tree.logicalRestriction.restrictions;

        if (dropped.tree.logicalRestriction || dropped.parent.logicalRestriction) {
            var draggedParent = dragged.parent.logicalRestriction.restrictions,
                droppedParent = dropped.parent 
                    ? dropped.parent.logicalRestriction.restrictions 
                    : [],
                draggedIndex = draggedParent.indexOf(dragged.tree),
                droppedIndex = droppedParent.indexOf(dropped.tree),
                draggedItem = angular.copy(dragged.tree);

            if (dropped.tree.logicalRestriction) {
                dropped.tree.logicalRestriction.restrictions.splice(droppedIndex+1, 0, draggedItem);
            } else {
                droppedParent.splice(droppedIndex+1, 0, draggedItem);
            }

            draggedParent.splice(draggedParent.indexOf(dragged.tree), 1);
        }
    }

    vm.clickTreeMode = function(value) {
        vm.treeMode = value;

        vm.restriction = QueryStore[value + 'Restriction'];
        vm.tree = vm.getTree();
        vm.setCurrentSavedTree();
        console.log('clickTreeMode', vm.treeMode, value + 'Restriction', vm.restriction, vm.tree);
    }

    vm.init();
});