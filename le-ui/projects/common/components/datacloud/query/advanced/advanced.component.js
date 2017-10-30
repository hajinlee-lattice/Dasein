angular.module('common.datacloud.query.builder', [
    'common.datacloud.query.builder.input',
    'common.datacloud.query.builder.tree'
])
.controller('AdvancedQueryCtrl', function(
    $state, $stateParams, $timeout, $q, QueryStore, $scope, QueryService,
    SegmentStore, DataCloudStore, Cube, RatingsEngineStore, 
    RatingEngineModel, CurrentRatingEngine
) {
    var vm = this, CoverageMap;

    angular.extend(this, {
        inModel: $state.current.name.split('.')[1] === 'model',
        mode: RatingEngineModel !== null ? 'rules' : 'segment',
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
        buckets: [],
        bucketsMap: {'A':0,'A-':1,'B':2,'C':3,'D':4,'F':5},
        bucketLabels: ['A','A-','B','C','D','F'],
        default_bucket: 'A',
        rating_rule: {},
        coverage_map: {},
        rating_id: $stateParams.rating_id,
        ratings: RatingsEngineStore ? RatingsEngineStore.ratings : null,
        treeMode: 'account'
    });

    vm.init = function() {
        console.log('[AQB] RatingEngineModel:', RatingEngineModel);

        if (vm.mode == 'rules') {
            vm.rating_rule = RatingEngineModel.rule.ratingRule;
            vm.rating_buckets = vm.rating_rule.bucketToRuleMap;
            vm.default_bucket = vm.rating_rule.defaultBucketName;

            RatingsEngineStore.setRule(RatingEngineModel)

            vm.initCoverageMap();

            RatingsEngineStore.getCoverageMap(RatingEngineModel, CurrentRatingEngine.segment.name).then(function(result) {
                CoverageMap = vm.initCoverageMap(result);
                console.log('[AQB] CoverageMap:', CoverageMap);
            }); 

            vm.getRuleRecordCounts();
        }

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

                console.log('[AQB] Restriction:', angular.copy(vm.restriction));
                console.log('[AQB] Items:', vm.items);
                console.log('[AQB] Cube:', vm.cube);
            }, 1);
        });
    }

    vm.initCoverageMap = function(map) {
        var n = (map ? 0 : -1);

        vm.buckets = [];

        vm.bucketLabels.forEach(function(bucketName, index) {
            vm.buckets.push({ bucket: bucketName,  count: n }); 
        });

        if (map) {
            var segmentId = Object.keys(map.segmentIdModelRulesCoverageMap)[0];

            vm.coverage_map = map.segmentIdModelRulesCoverageMap[segmentId];

            if (vm.coverage_map) {
                vm.coverage_map.bucketCoverageCounts.forEach(function(bkt) {
                    vm.buckets[vm.bucketsMap[bkt.bucket]].count = bkt.count;
                });
            }
        }

        return map;
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
        var bucketRestrictions = [];
        
        RatingEngineModel.rule.selectedAttributes
        .forEach(function(value, index) {
            var item = angular.copy(vm.enrichments[vm.enrichmentsMap[value]]);

            if (item) {
                bucketRestrictions.push({
                    bucketRestriction: {
                        attr: item.Entity + '.' + value,
                        bkt: {}
                    }
                });
            }
        });

        if (vm.bucket) {
            var bucket = vm.rating_rule.bucketToRuleMap[vm.bucket],
                fromBucket = bucket[vm.treeMode + '_restriction'],
                restrictions = fromBucket.logicalRestriction.restrictions,
                allBuckets = vm.recursiveGetBucketRestrictions(restrictions),
                ids = [];


            allBuckets.forEach(function(value, index) {
                ids.push(value.bucketRestriction.attr);
            })

            bucketRestrictions.forEach(function(value, index) {
                if (ids.indexOf(value.bucketRestriction.attr) < 0) {
                    restrictions.push(value);
                }
            })
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

        if (!vm.compareTree(old, current)) {
            vm.history.push(current);

            if (!noCount) {
                vm.updateCount();
            }
        }
    }

    vm.changeDefaultBucket = function(bucket) {
        vm.updateCount();
    }

    vm.clickBucketTile = function(bucket) {
        vm.labelIncrementor = 0;
        vm.bucket = bucket.bucket;
        vm.tree = vm.getTree();
    }

    vm.getRuleCount = function(bkt) {
        if (bkt) {
            var buckets = [
                vm.rating_rule.bucketToRuleMap[bkt.bucket] 
            ];
        } else {
            var buckets = [];

            vm.bucketLabels.forEach(function(bucketName, index) {
                buckets.push(vm.rating_rule.bucketToRuleMap[bucketName]); 
            });
        }

        var filtered = [], restrictions = [];

        buckets.forEach(function(bucket, index) {
            restrictions = vm.recursiveGetBucketRestrictions(bucket[vm.treeMode + '_restriction'].logicalRestriction.restrictions);
            
            filtered = filtered.concat(restrictions.filter(function(value, index) {
                return value.bucketRestriction && value.bucketRestriction.bkt && value.bucketRestriction.bkt.Id;
            }));
        })

        return filtered.length;
    }
    
    vm.clickUndo = function() {
        var lastState;

        while (lastState = vm.history.pop()) {
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
        vm.prevBucketCountAttr = null;

        if (vm.mode == 'rules') {
            QueryStore.counts[vm.treeMode + 's'].loading = true;

            var RatingEngineCopy = angular.copy(RatingEngineModel),
                BucketMap = RatingEngineCopy.rule.ratingRule.bucketToRuleMap;

            vm.bucketLabels.forEach(function(bucketName, index) {
                var logical = BucketMap[bucketName][vm.treeMode + '_restriction'].logicalRestriction;

                logical.restrictions = logical.restrictions.filter(function(restriction, index) {
                    return restriction.bucketRestriction && restriction.bucketRestriction.bkt.Id;
                });

                vm.buckets[vm.bucketsMap[bucketName]].count = -1;

                SegmentStore.sanitizeSegmentRestriction([ BucketMap[bucketName][vm.treeMode + '_restriction'] ]);
            });

            $timeout(function() {
                RatingsEngineStore.getCoverageMap(RatingEngineCopy, CurrentRatingEngine.segment.name).then(function(result) {
                    vm.initCoverageMap(result);
                }); 
            }, 250);
        } else {
            if (QueryStore.counts['accounts']) {
                QueryStore.counts['accounts'].loading = true;
            }

            if (QueryStore.counts['contacts']) {
                QueryStore.counts['contacts'].loading = true;
            }

            $timeout(function() {
                var segment = { 
                    'free_form_text_search': "",
                    'page_filter': {
                        'num_rows': 10,
                        'row_offset': 0
                    }
                };

                segment['account_restriction'] = angular.copy(QueryStore.accountRestriction);
                segment['contact_restriction'] = angular.copy(QueryStore.contactRestriction);

                QueryService.GetCountByQuery('accounts', SegmentStore.sanitizeSegment(segment)).then(function(result) {
                    QueryStore.setResourceTypeCount('accounts', false, result);
                });


                QueryService.GetCountByQuery('contacts', SegmentStore.sanitizeSegment(segment)).then(function(result) {
                    QueryStore.setResourceTypeCount('contacts', false, result);
                });
            }, 250);
        }
    }

    vm.updateBucketCount = function(bucketRestriction) {
        var deferred = $q.defer();

        var segment = {
            "free_form_text_search": ""
        };

        segment[vm.treeMode + '_restriction'] = {
            "restriction": {
                "bucketRestriction": angular.copy(bucketRestriction)
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

    vm.getRuleRecordCounts = function(restrictions) {
        var restrictions = restrictions || vm.getAllBucketRestrictions(),
            segmentId = CurrentRatingEngine.segment.name,
            map = {};

        restrictions.forEach(function(bucket, index) {
            bucket.bucketRestriction.bkt.Cnt = -1;

            map[bucket.bucketRestriction.attr + '_' + index] = bucket;
        })

// console.log('getRuleRecordCounts', restrictions);
        RatingsEngineStore.getBucketRuleCounts(angular.copy(restrictions), segmentId).then(function(result) {
            var buckets = result.segmentIdAndSingleRulesCoverageMap;
            
            Object.keys(buckets).forEach(function(key) {
                var label = map[key].bucketRestriction.attr,
                    type = label.split('.')[0] == 'Contact' ? 'contact' : 'account';
                
                map[key].bucketRestriction.bkt.Cnt = buckets[key][type + 'Count'];
            });
        }); 
    }

    vm.getAllBucketRestrictions = function() {
        var RatingEngineCopy = RatingEngineModel,
            BucketMap = RatingEngineCopy.rule.ratingRule.bucketToRuleMap,
            restrictions = [];

        vm.bucketLabels.forEach(function(bucketName, index) {
            var logical = BucketMap[bucketName][vm.treeMode + '_restriction'].logicalRestriction;

            vm.recursiveGetBucketRestrictions(logical.restrictions, restrictions);
        });

        return restrictions;
    }

    vm.recursiveGetBucketRestrictions = function(tree, restrictions) {
        restrictions = restrictions || [];

        tree.forEach(function(branch) {
            if (branch && branch.bucketRestriction && branch.bucketRestriction && branch.bucketRestriction.bkt.Id) {
                restrictions.push(branch);
            }

            if (branch && branch.logicalRestriction) {
                vm.recursiveGetBucketRestrictions(branch.logicalRestriction.restrictions, restrictions);
            }
        });

        return restrictions;
    };

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
        if (vm.mode == 'rules') {
            var state = 'home.ratingsengine.wizard.segment.attributes';
        } else {
            var state = vm.inModel
                    ? 'home.model.analysis.explorer.attributes'
                    : 'home.segment.explorer.attributes';
        }

        $state.go(state);
    }

    vm.dropMoveItem = function(dragged, dropped, endMove) {
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
                dropped.tree.logicalRestriction.restrictions.splice(droppedIndex + 1, 0, draggedItem);
            } else {
                droppedParent.splice(droppedIndex + 1, 0, draggedItem);
            }

            draggedParent.splice(draggedParent.indexOf(dragged.tree), 1);
        }
    }

    vm.clickTreeMode = function(value) {
        vm.treeMode = value;

        vm.restriction = QueryStore[value + 'Restriction'];
        vm.tree = vm.getTree();

        vm.setCurrentSavedTree();
    }

    vm.mouseOut = function() {
        vm.draggedItem = null;
        vm.droppedItem = null;
    }

    vm.init();
});