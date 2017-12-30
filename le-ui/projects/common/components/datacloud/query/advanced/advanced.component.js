angular.module('common.datacloud.query.builder', [
    'common.datacloud.query.builder.input',
    'common.datacloud.query.builder.tree'
])
.controller('AdvancedQueryCtrl', function(
    $state, $stateParams, $timeout, $q, $rootScope, Cube, 
    QueryStore, QueryService, SegmentStore, DataCloudStore,
    RatingsEngineStore, RatingEngineModel, CurrentRatingEngine
) {
    var vm = this, CoverageMap;

    angular.extend(this, {
        inModel: $state.current.name.split('.')[1] === 'model',
        mode: RatingEngineModel !== null ? 'rules' : 'segment',
        cube: Cube,
        history: QueryStore.history,
        restriction: QueryStore.accountRestriction,
        account_restriction: QueryStore.accountRestriction,
        contact_restriction: QueryStore.contactRestriction,
        enrichmentsMap: DataCloudStore.getEnrichmentsMap(),
        droppedItem: null,
        draggedItem: null,
        items: [],
        enrichments: [],
        labelIncrementor: 0,
        bucket: 'A+',
        buckets: [],
        bucketsMap: {'A+':0,'A':1,'B':2,'C':3,'D':4,'F':5},
        bucketLabels: ['A+','A','B','C','D','F'],
        default_bucket: 'A+',
        rating_rule: {},
        coverage_map: {},
        rating_id: $stateParams.rating_id,
        ratings: RatingsEngineStore ? RatingsEngineStore.ratings : null,
        treeMode: 'account',
        segment: $stateParams.segment,
        segmentInputTree: [],
        rulesInputTree: [],
        accountRulesTree: [],
        contactRulesTree: []
    });

    vm.init = function() {
        console.log('[AQB] RatingEngineModel:', RatingEngineModel);

        if (vm.segment != null && vm.segment != "Create"){
            SegmentStore.getSegmentByName(vm.segment).then(function(result) {
                vm.displayName = result.display_name;

                $rootScope.$broadcast('header-back', { 
                    path: '^home.segment.accounts',
                    displayName: vm.displayName,
                    sref: 'home.segments'
                });
            });    
        }

        if (vm.mode == 'rules') {
            vm.rating_rule = RatingEngineModel.rule.ratingRule;
            vm.rating_buckets = vm.rating_rule.bucketToRuleMap;
            vm.default_bucket = vm.rating_rule.defaultBucketName;

            RatingsEngineStore.setRule(RatingEngineModel)

            vm.initCoverageMap();
            vm.getRatingsAndRecordCounts(RatingEngineModel, CurrentRatingEngine.segment.name);

            console.log('[AQB] CoverageMap:', CoverageMap);
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
                
                if (vm.mode == 'rules') {
                    vm.accountRulesTree = [vm.generateRulesTreeForEntity('Account')];
                    vm.contactRulesTree = [vm.generateRulesTreeForEntity('Contact')];

                    vm.rulesInputTree = {
                        'collapsed': false,
                        'logicalRestriction': {
                            'operator': 'AND',
                            'restrictions': [
                                vm.accountRulesTree[0],
                                vm.contactRulesTree[0]
                            ]
                        }
                    };
                }
                
                vm.setCurrentSavedTree();

                console.log('[AQB] Restriction:', angular.copy(vm.restriction));
                console.log('[AQB] Items:', vm.items);
                console.log('[AQB] Cube:', vm.cube);
            }, 1);
        });

        QueryStore.setAddBucketTreeRoot(null);

        if (vm.mode == 'segment') {
            vm.segmentInputTree = {
                'logicalRestriction': {
                    'operator': 'AND',
                    'restrictions': [
                        vm.account_restriction.restriction,
                        vm.contact_restriction.restriction
                    ]
                }
            };
        }
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

    vm.getAccountTree = function() {
        return [ vm.account_restriction.restriction ];
    }

    vm.getContactTree = function() {
        return [ vm.contact_restriction.restriction ];
    }

    vm.getSegmentInputTree = function() {
        if (vm.account_restriction.restriction.logicalRestriction.restrictions.length != 0 && vm.contact_restriction.restriction.logicalRestriction.restrictions.length != 0) {
            return [ vm.segmentInputTree ];
        } else if (vm.account_restriction.restriction.logicalRestriction.restrictions.length && !vm.contact_restriction.restriction.logicalRestriction.restrictions.length) {
            return vm.getAccountTree();
        } else if (!vm.account_restriction.restriction.logicalRestriction.restrictions.length && vm.contact_restriction.restriction.logicalRestriction.restrictions.length) {
            return vm.getContactTree();
        }
    }

    vm.getRulesInputTree = function() {
        var accountAttrSelected = vm.checkAttributesSelected('account');
        var contactAttrSelected = vm.checkAttributesSelected('contact');

        if (vm.accountRulesTree[0] && vm.contactRulesTree[0]) {
            if (vm.accountRulesTree[0].logicalRestriction.restrictions.length != 0 && vm.contactRulesTree[0].logicalRestriction.restrictions.length != 0 && 
                    accountAttrSelected && contactAttrSelected) {

                return [ vm.rulesInputTree ];
            } else if (vm.accountRulesTree[0].logicalRestriction.restrictions.length && vm.contactRulesTree[0].logicalRestriction.restrictions.length == 0 || 
                        (accountAttrSelected && !contactAttrSelected)) {

                return vm.accountRulesTree;
            } else if (vm.contactRulesTree[0].logicalRestriction.restrictions.length && vm.accountRulesTree[0].logicalRestriction.restrictions.length == 0 || 
                        (contactAttrSelected && !accountAttrSelected)) {

                return vm.contactRulesTree;
            }
        }
    }

    vm.checkAttributesSelected = function(entity) {
        var bucket = vm.buckets[vm.bucketsMap[vm.bucket]];
        var counts = vm.getRuleCount(bucket, true);

        return counts[entity] > 0;
    }

    vm.resetRulesInputTree = function() {
        vm.rulesInputTree = {
            'collapsed': false,
            'logicalRestriction': {
                'operator': 'AND',
                'restrictions': [
                    vm.accountRulesTree[0],
                    vm.contactRulesTree[0]
                ]
            }
        };
    }


    // vm.generateRulesTree = function() {
    //     var bucketRestrictions = [];
        
    //     RatingEngineModel.rule.selectedAttributes
    //     .forEach(function(value, index) {
    //         var item = angular.copy(vm.enrichments[vm.enrichmentsMap[value]]);

    //         if (item) {
    //             bucketRestrictions.push({
    //                 bucketRestriction: {
    //                     attr: item.Entity + '.' + value,
    //                     bkt: {}
    //                 }
    //             });
    //         }
    //     });
    //     if (vm.bucket) {
    //         var bucket = vm.rating_rule.bucketToRuleMap[vm.bucket],
    //             fromBucket = bucket[vm.treeMode + '_restriction'],
    //             restrictions = fromBucket.logicalRestriction.restrictions,
    //             setBuckets = QueryStore.getAllBuckets(restrictions),
    //             allBuckets = QueryStore.getAllBuckets(restrictions, null, true),
    //             ids = [],
    //             rids = [];


    //         setBuckets.forEach(function(value, index) {
    //             ids.push(value.bucketRestriction.attr);
    //         })

    //         allBuckets.forEach(function(value, index) {
    //             rids.push(value.bucketRestriction.attr);
    //         })

    //         bucketRestrictions.forEach(function(value, index) {
    //             if (ids.indexOf(value.bucketRestriction.attr) < 0 && rids.indexOf(value.bucketRestriction.attr) < 0) {
    //                 restrictions.push(value);
    //             }
    //         })
    //     }

    //     return fromBucket;
    // }

    vm.generateRulesTreeForEntity = function(entity) {
        var bucketRestrictions = [];
        
        RatingEngineModel.rule.selectedAttributes
        .forEach(function(value, index) {
            var item = angular.copy(vm.enrichments[vm.enrichmentsMap[value]]);

            if (item && item.Entity == entity) {
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
                fromBucket = bucket[entity.toLowerCase() + '_restriction'],
                restrictions = fromBucket.logicalRestriction.restrictions,
                setBuckets = QueryStore.getAllBuckets(restrictions),
                allBuckets = QueryStore.getAllBuckets(restrictions, null, true),
                ids = [],
                rids = [];


            setBuckets.forEach(function(value, index) {
                ids.push(value.bucketRestriction.attr);
            })

            allBuckets.forEach(function(value, index) {
                rids.push(value.bucketRestriction.attr);
            })

            bucketRestrictions.forEach(function(value, index) {
                if (ids.indexOf(value.bucketRestriction.attr) < 0 && rids.indexOf(value.bucketRestriction.attr) < 0) {
                    restrictions.push(value);
                }
            })
        }

        return fromBucket;
    }

    vm.pushItem = function(item, tree) {
        if (item) {
            
            var attributeEntity = item.Entity,
                cube = vm.cube[attributeEntity].Stats[item.ColumnId];

            item.cube = cube;
            item.topbkt = tree.bkt;

            vm.items.push(item);
        }
    }

    vm.setCurrentSavedTree = function() {
        QueryStore.currentSavedTree = vm.mode == 'segment' ?  angular.copy([ vm.segmentInputTree ]) : angular.copy([ vm.rulesInputTree ]);

    }

    vm.getBucketLabel = function(bucket) {
        if (QueryStore.getPublic()['resetLabelIncrementor']) {
            vm.labelIncrementor = 0;
            QueryStore.setPublicProperty('resetLabelIncrementor', false);
        }

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

        var current = vm.mode == 'segment' ?  angular.copy([ vm.segmentInputTree ]) : angular.copy([ vm.rulesInputTree ]),
            old = angular.copy(vm.history[vm.history.length -1]) || [];
        console.log('saveState', current);
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
        console.log('CLICK BUCKET TILE', bucket);
        vm.labelIncrementor = 0;
        vm.bucket = bucket.bucket;
        // vm.tree = vm.getTree();
        vm.accountRulesTree = [ vm.generateRulesTreeForEntity('Account') ];
        vm.contactRulesTree = [ vm.generateRulesTreeForEntity('Contact') ];
        vm.resetRulesInputTree();
    }

    vm.getRuleCount = function(bkt, entity) {
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

        var accountRestrictions = [], contactRestrictions = [];
        var filteredAccounts = [], filteredContacts = []

        buckets.forEach(function(bucket, index) {
            accountRestrictions = QueryStore.getAllBuckets(bucket['account_restriction'].logicalRestriction.restrictions);
            contactRestrictions = QueryStore.getAllBuckets(bucket['contact_restriction'].logicalRestriction.restrictions);

            filteredAccounts = filteredAccounts.concat(accountRestrictions.filter(function(value, index) {
                return value.bucketRestriction && value.bucketRestriction.bkt && value.bucketRestriction.bkt.Id;
            }));

            filteredContacts = filteredContacts.concat(contactRestrictions.filter(function(value, index) {
                return value.bucketRestriction && value.bucketRestriction.bkt && value.bucketRestriction.bkt.Id;
            }));
        })

        if (entity) {
            var counts = {
                        'account': filteredAccounts.length, 
                        'contact': filteredContacts.length
                    };
            return counts;
        }

        return filteredAccounts.length + filteredContacts.length;
    }

    vm.getRatingsAndRecordCounts = function(model, segmentName) {
        var rulesForCounts = vm.getRuleRecordCounts();
                console.log('getRatingsAndRecordCounts', segmentName, rulesForCounts, model);

        RatingsEngineStore.getCoverageMap(model, segmentName, rulesForCounts).then(function(result) {
            CoverageMap = vm.initCoverageMap(result);

            var buckets = result.segmentIdAndSingleRulesCoverageMap;
            
            Object.keys(buckets).forEach(function(key) {
                var label = vm.RuleRecordMap[key].bucketRestriction.attr,
                    type = label.split('.')[0] == 'Contact' ? 'contact' : 'account';
                
                vm.RuleRecordMap[key].bucketRestriction.bkt.Cnt = buckets[key][type + 'Count'];
            });
        });
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


    // vm.setState = function(newState) {
    //     console.log('set',newState);

    //     if (!vm.compareTree(newState, angular.copy(vm.tree))) {
    //         vm.labelIncrementor = 0;
            
    //         QueryStore[vm.treeMode + 'Restriction'].restriction = newState[0];

    //         vm.restriction = {
    //             restriction: newState[0]
    //         };

    //         vm.tree = newState;
    //         // vm.mergedTree.logicalRestriction.restrictions[0] = newState;

    //         return true;
    //     }

    //     return false;
    // }

    vm.setState = function(newState) {
        console.log('SET', newState);
        // var currentTree = vm.mode == 'rules' ? vm.getRulesInputTree : vm.getSegmentInputTree();
        if (!vm.compareTree(newState, vm.getSegmentInputTree())) {
            vm.labelIncrementor = 0;
            QueryStore['accountRestriction'].restriction = newState[0].logicalRestriction.restrictions[0];
            QueryStore['contactRestriction'].restriction = newState[0].logicalRestriction.restrictions[1];

            vm.account_restriction = {
                restriction: newState[0].logicalRestriction.restrictions[0]
            };

            vm.contact_restriction = {
                restriction: newState[0].logicalRestriction.restrictions[1]
            };

            vm.segmentInputTree.logicalRestriction.restrictions[0] = newState[0].logicalRestriction.restrictions[0];
            vm.segmentInputTree.logicalRestriction.restrictions[1] = newState[0].logicalRestriction.restrictions[1];

            return true;
        }

        return false;
    }

    vm.updateCount = function() {
        vm.prevBucketCountAttr = null;
        QueryStore.setPublicProperty('enableSaveSegmentButton', true);

        if (vm.mode == 'rules') {
            QueryStore.counts[vm.treeMode + 's'].loading = true;

            var RatingEngineCopy = angular.copy(RatingEngineModel),
                BucketMap = RatingEngineCopy.rule.ratingRule.bucketToRuleMap;

            vm.bucketLabels.forEach(function(bucketName, index) {
                // var logical = BucketMap[bucketName][vm.treeMode + '_restriction'].logicalRestriction;

                //logical.restrictions = logical.restrictions.filter(function(restriction, index) {
                //    return restriction.bucketRestriction && restriction.bucketRestriction.bkt.Id;
                //});
                
                vm.buckets[vm.bucketsMap[bucketName]].count = -1;

                // SegmentStore.sanitizeSegmentRestriction([ BucketMap[bucketName][vm.treeMode + '_restriction'] ]);
                SegmentStore.sanitizeSegmentRestriction([ BucketMap[bucketName]['account_restriction'] ]);
                SegmentStore.sanitizeSegmentRestriction([ BucketMap[bucketName]['contact_restriction'] ]);

            });

            $timeout(function() {
                vm.getRatingsAndRecordCounts(RatingEngineCopy, CurrentRatingEngine.segment.name);
            }, 250);
        } else {
            QueryStore.setEntitiesProperty('loading', true);
            $timeout(function() {
                var segment = { 
                    'free_form_text_search': "",
                    'page_filter': {
                        'num_rows': 10,
                        'row_offset': 0
                    }
                };
                vm.labelIncrementor = 0;

                segment['account_restriction'] = angular.copy(QueryStore.accountRestriction);

                segment['contact_restriction'] = angular.copy(QueryStore.contactRestriction);

                QueryStore.getEntitiesCounts(SegmentStore.sanitizeSegment(segment)).then(function(result) {
                    QueryStore.setResourceTypeCount('accounts', false, result['Account']);
                    QueryStore.setResourceTypeCount('contacts', false, result['Contact']);
                });
            }, 250);
        }
    }

    vm.updateBucketCount = function(bucketRestriction) {
        var deferred = $q.defer();

        var segment = {
            "free_form_text_search": ""
        };

        vm.treeMode = bucketRestriction.attr.split('.')[0].toLowerCase();
        segment[vm.treeMode + '_restriction'] = {
            "restriction": {
                "bucketRestriction": angular.copy(bucketRestriction)
            }
        };
        if(vm.treeMode === 'purchasehistory'){
            vm.treeMode = 'account';
        }
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
            segmentId = CurrentRatingEngine.segment.name;
        
        vm.RuleRecordMap = {};

        restrictions.forEach(function(bucket, index) {
            bucket.bucketRestriction.bkt.Cnt = -1;

            vm.RuleRecordMap[bucket.bucketRestriction.attr + '_' + index] = bucket;
        })

        return RatingsEngineStore.getBucketRuleCounts(angular.copy(restrictions), segmentId); 
    }

    vm.getAllBucketRestrictions = function() {
        var RatingEngineCopy = RatingEngineModel,
            BucketMap = RatingEngineCopy.rule.ratingRule.bucketToRuleMap,
            restrictions = [];

        vm.bucketLabels.forEach(function(bucketName, index) {
            // var logical = BucketMap[bucketName][vm.treeMode + '_restriction'].logicalRestriction;

            // QueryStore.getAllBuckets(logical.restrictions, restrictions);

            var accountLogical = BucketMap[bucketName]['account_restriction'].logicalRestriction;
            var contactLogical = BucketMap[bucketName]['contact_restriction'].logicalRestriction;

            QueryStore.getAllBuckets(accountLogical.restrictions, restrictions);
            QueryStore.getAllBuckets(contactLogical.restrictions, restrictions);
        });
        console.log('ALL BUCKET RESTRICTIONS', restrictions);
        return restrictions;
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
        SegmentStore.sanitizeSegmentRestriction(angular.copy(current));

        return (JSON.stringify(old) === JSON.stringify(current));
    }

    vm.checkDisableSave = function() {
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
            var state = 'home.ratingsengine.wizard.segment.attributes.add';
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