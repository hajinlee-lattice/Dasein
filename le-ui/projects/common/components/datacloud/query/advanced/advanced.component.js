angular.module('common.datacloud.query.builder', [
    'common.datacloud.query.builder.input',
    'common.datacloud.query.builder.tree'
])
    .controller('AdvancedQueryCtrl', function (
        $state, $stateParams, $timeout, $q, $rootScope, Cube,
        QueryStore, QueryService, SegmentStore, DataCloudStore,
        RatingsEngineStore, RatingEngineModel, CurrentRatingEngine
    ) {
        var vm = this, CoverageMap;

        angular.extend(this, {
            inModel: $state.current.name.split('.')[1] === 'model',
            inRatingEngine: (CurrentRatingEngine !== null),
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
            bucket: QueryStore.getSelectedBucket(),
            buckets: [],
            bucketsMap: { 'A': 0, 'B': 1, 'C': 2, 'D': 3, 'E': 4, 'F': 5 },
            bucketLabels: ['A', 'B', 'C', 'D', 'E', 'F'],
            default_bucket: 'A',
            rating_rule: {},
            coverage_map: {},
            rating_id: $stateParams.rating_id,
            ratings: RatingsEngineStore ? RatingsEngineStore.ratings : null,
            treeMode: 'account',
            segment: $stateParams.segment,
            segmentInputTree: [],
            rulesInputTree: [],
            accountRulesTree: [],
            contactRulesTree: [],
            mouseDownTimer: false,
            heights: {}
        });

        vm.init = function () {
            // console.log('[AQB] RatingEngineModel:', RatingEngineModel);
            if ($state.current.name === 'home.ratingsengine.rulesprospects.segment.attributes.rules') {
                vm.mode = 'rules';
            } else if ($state.current.name === 'home.ratingsengine.dashboard.segment.attributes.rules') {
                vm.mode = 'dashboardrules';
            } else {
                vm.mode = 'segment';
            }

            QueryStore.mode = vm.mode;

            if (vm.mode == 'rules' || vm.mode == 'dashboardrules') {
                vm.ratingEngineModel = RatingEngineModel;
                vm.rating_rule = RatingEngineModel.rule.ratingRule;
                vm.rating_buckets = vm.rating_rule.bucketToRuleMap;
                vm.default_bucket = vm.rating_rule.defaultBucketName;

                RatingsEngineStore.setRule(RatingEngineModel);

                vm.initCoverageMap();
                vm.getRatingsAndRecordCounts(RatingEngineModel, CurrentRatingEngine.segment.name);

                QueryStore.setAccountBucketTreeRoot(vm.accountRulesTree[0]);
                QueryStore.setContactBucketTreeRoot(vm.contactRulesTree[0]);
                // console.log('[AQB] CoverageMap:', CoverageMap);
            }

            DataCloudStore.getEnrichments().then(function (enrichments) {
                for (var i = 0, enrichment; i < enrichments.length; i++) {
                    enrichment = enrichments[i];

                    if (!enrichment) {
                        continue;
                    }

                    vm.enrichmentsMap[enrichment.Entity + '.' + enrichment.ColumnId] = i;
                }

                vm.enrichments = enrichments;

                DataCloudStore.setEnrichmentsMap(vm.enrichmentsMap);

                $timeout(function () {

                    if (vm.mode == 'rules' || vm.mode == 'dashboardrules') {

                        vm.setRulesTree();

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

                    // console.log('[AQB] Restriction:', angular.copy(vm.restriction));
                    // console.log('[AQB] Items:', vm.items);
                    // console.log('[AQB] Cube:', vm.cube);
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

        vm.initCoverageMap = function (map) {
            var n = (map ? 0 : -1);

            vm.buckets = [];

            vm.bucketLabels.forEach(function (bucketName, index) {
                vm.buckets.push({ bucket: bucketName, count: n });
            });

            if (map) {
                var segmentId = Object.keys(map.segmentIdModelRulesCoverageMap)[0];

                vm.coverage_map = map.segmentIdModelRulesCoverageMap[segmentId];

                if (vm.coverage_map) {
                    vm.coverage_map.bucketCoverageCounts.forEach(function (bkt) {
                        vm.buckets[vm.bucketsMap[bkt.bucket]].count = bkt.count;
                    });
                }
            }

            return map;
        }

        vm.getTree = function () {
            console.log('Get tree');
            switch (vm.mode) {
                case 'segment':
                    return [vm.restriction.restriction];
                case 'rules':
                    return [vm.generateRulesTree()];
                case 'dashboardrules':
                    return [vm.generateRulesTree()];
            }
        }

        vm.getAccountTree = function () {
            return [vm.account_restriction.restriction];
        }

        vm.getContactTree = function () {
            return [vm.contact_restriction.restriction];
        }

        vm.setRulesTree = function () {
            vm.accountRulesTree = [vm.generateRulesTreeForEntity('Account')];
            vm.contactRulesTree = [vm.generateRulesTreeForEntity('Contact')];
            QueryStore.setAccountBucketTreeRoot(vm.accountRulesTree[0]);
            QueryStore.setContactBucketTreeRoot(vm.contactRulesTree[0]);
        }

        vm.getSegmentInputTree = function () {
            if (vm.account_restriction.restriction.logicalRestriction.restrictions.length != 0 && vm.contact_restriction.restriction.logicalRestriction.restrictions.length != 0) {
                return [vm.segmentInputTree];
            } else if (vm.account_restriction.restriction.logicalRestriction.restrictions.length && !vm.contact_restriction.restriction.logicalRestriction.restrictions.length) {
                return vm.getAccountTree();
            } else if (!vm.account_restriction.restriction.logicalRestriction.restrictions.length && vm.contact_restriction.restriction.logicalRestriction.restrictions.length) {
                return vm.getContactTree();
            }
        }

        vm.getRulesInputTree = function () {

            var accountAttrSelected = vm.checkAttributesSelected('account');
            var contactAttrSelected = vm.checkAttributesSelected('contact');

            if (vm.accountRulesTree[0] && vm.contactRulesTree[0]) {
                if (vm.accountRulesTree[0].logicalRestriction.restrictions.length != 0 && vm.contactRulesTree[0].logicalRestriction.restrictions.length != 0 &&
                    accountAttrSelected && contactAttrSelected) {

                    return [vm.rulesInputTree];
                } else if (vm.accountRulesTree[0].logicalRestriction.restrictions.length && vm.contactRulesTree[0].logicalRestriction.restrictions.length == 0 ||
                    (accountAttrSelected && !contactAttrSelected)) {

                    return vm.accountRulesTree;
                } else if (vm.contactRulesTree[0].logicalRestriction.restrictions.length && vm.accountRulesTree[0].logicalRestriction.restrictions.length == 0 ||
                    (contactAttrSelected && !accountAttrSelected)) {

                    return vm.contactRulesTree;
                }
            }
        }

        vm.checkAttributesSelected = function (entity) {
            var bucket = vm.buckets[vm.bucketsMap[vm.bucket]];
            var counts = vm.getRuleCount(bucket, true);

            return counts[entity] > 0;
        }

        vm.resetRulesInputTree = function () {
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

        vm.saveRules = function () {
            vm.saved = true;
            RatingsEngineStore.nextSaveRules();
        }


        vm.generateRulesTree = function () {
            var bucketRestrictions = [];

            if (vm.bucket) {
                var bucket = vm.rating_rule.bucketToRuleMap[vm.bucket],
                    fromBucket = bucket[vm.treeMode + '_restriction'],
                    restrictions = fromBucket.logicalRestriction.restrictions,
                    setBuckets = QueryStore.getAllBuckets(restrictions),
                    allBuckets = QueryStore.getAllBuckets(restrictions, null, true),
                    ids = [],
                    rids = [];


                setBuckets.forEach(function (value, index) {
                    ids.push(value.bucketRestriction.attr);
                })

                allBuckets.forEach(function (value, index) {
                    rids.push(value.bucketRestriction.attr);
                })

                bucketRestrictions.forEach(function (value, index) {
                    if (ids.indexOf(value.bucketRestriction.attr) < 0 && rids.indexOf(value.bucketRestriction.attr) < 0) {
                        restrictions.push(value);
                    }
                })
            }

            return fromBucket;
        }

        /**
         * TODO: A map with association Entity -> Bucket should be created
         * Look at vm.updateBucketCount where we can use this map as well
         * @param {*} item 
         * @param {*} entity 
         */
        vm.isMatching = function (itemEntity, entity) {
            if (itemEntity == entity) {
                return true;
            }
            else if ((itemEntity === 'PurchaseHistory' && entity === 'Account') ||
                (itemEntity === 'Rating' && entity === 'Account')) {
                return true;
            } else {
                return false;
            }
        }
        vm.generateRulesTreeForEntity = function (entity) {
            var bucketRestrictions = [];

            if (vm.bucket) {
                var bucket = vm.rating_rule.bucketToRuleMap[vm.bucket],
                    fromBucket = bucket[entity.toLowerCase() + '_restriction'],
                    restrictions = fromBucket.logicalRestriction.restrictions,
                    setBuckets = QueryStore.getAllBuckets(restrictions),
                    allBuckets = QueryStore.getAllBuckets(restrictions, null, true),
                    ids = [],
                    rids = [];

                setBuckets.forEach(function (value, index) {
                    ids.push(value.bucketRestriction.attr);
                });

                allBuckets.forEach(function (value, index) {
                    rids.push(value.bucketRestriction.attr);
                });

                bucketRestrictions.forEach(function (value, index) {
                    if (ids.indexOf(value.bucketRestriction.attr) < 0 && rids.indexOf(value.bucketRestriction.attr) < 0) {
                        restrictions.push(value);
                    }
                });
            }

            return fromBucket;
        }

        vm.pushItem = function (item, tree) {
            if (item) {
                var attributeEntity = item.Entity,
                    cube = vm.cube[attributeEntity].Stats[item.ColumnId];

                item.cube = cube;
                item.topbkt = tree.bkt;

                vm.items.push(item);
            }
        }

        vm.setCurrentSavedTree = function () {
            QueryStore.currentSavedTree = vm.mode == 'segment' ? angular.copy([vm.segmentInputTree]) : angular.copy([vm.rulesInputTree]);
        }

        vm.getBucketLabel = function (bucket) {
            if (QueryStore.getPublic()['resetLabelIncrementor']) {
                vm.labelIncrementor = 0;
                QueryStore.setPublicProperty('resetLabelIncrementor', false);
            }

            if (bucket && bucket.labelGlyph) {
                return bucket.labelGlyph;
            } else {
                vm.labelIncrementor += 1;
                if (vm.labelIncrementor === 0) {
                    vm.labelIncrementor += 1;
                }
                bucket.labelGlyph = vm.labelIncrementor;
                return vm.labelIncrementor;
            }
        }

        vm.saveState = function (noCount) {
            vm.labelIncrementor = 0;

            var current = vm.mode == 'segment' ? angular.copy([vm.segmentInputTree]) : angular.copy([vm.rulesInputTree]),
                old = angular.copy(vm.history[vm.history.length - 1]) || [];

            if (!vm.compareTree(old, current)) {
                vm.history.push(current);

                if (!noCount) {
                    vm.updateCount();
                }
            }
        }

        vm.changeDefaultBucket = function (bucket) {
            vm.updateCount();
        }

        vm.clickBucketTile = function (bucket) {

            vm.labelIncrementor = 0;
            vm.bucket = bucket.bucket;

            vm.setSelectedBucket(vm.bucket);
            vm.setRulesTree();
            vm.resetRulesInputTree();
        }

        vm.setSelectedBucket = function (bucket) {
            QueryStore.setSelectedBucket(bucket);
        }

        vm.getRuleCount = function (bkt, entity) {
            return QueryStore.getRuleCount(bkt, vm.rating_rule.bucketToRuleMap, vm.bucketLabels, entity);
        }

        vm.getRatingsAndRecordCounts = function (model, segmentName) {
            var rulesForCounts = vm.getRuleRecordCounts();
            //console.log('getRatingsAndRecordCounts', segmentName, rulesForCounts, model);

            RatingsEngineStore.getCoverageMap(model, segmentName, rulesForCounts).then(function (result) {
                CoverageMap = vm.initCoverageMap(result);

                var buckets = result.segmentIdAndSingleRulesCoverageMap;
                if (buckets) {
                    Object.keys(buckets).forEach(function (key) {
                        if (vm.RuleRecordMap[key]) {
                            var label = vm.RuleRecordMap[key].bucketRestriction.attr,
                                type = label.split('.')[0] == 'Contact' ? 'contact' : 'account';

                            vm.RuleRecordMap[key].bucketRestriction.bkt.Cnt = buckets[key][type + 'Count'];
                        }
                    });
                }
            });
        }

        vm.clickUndo = function () {
            var lastState;
            while (lastState = vm.history.pop()) {
                if (vm.setState(lastState)) {
                    vm.updateCount();

                    break;
                }
            }
        }


        vm.setState = function (newState) {
            // console.log('SET', newState);
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

        vm.updateCount = function () {
            QueryStore.setPublicProperty('enableSaveSegmentButton', true);

            if (vm.mode == 'rules' || vm.mode == 'dashboardrules') {
                QueryStore.counts[vm.treeMode + 's'].loading = true;

                var RatingEngineCopy = angular.copy(RatingEngineModel),
                    BucketMap = RatingEngineCopy.rule.ratingRule.bucketToRuleMap;

                vm.bucketLabels.forEach(function (bucketName, index) {
                    vm.buckets[vm.bucketsMap[bucketName]].count = -1;

                    SegmentStore.removeEmptyBuckets([BucketMap[bucketName]['account_restriction']]);
                    SegmentStore.removeEmptyBuckets([BucketMap[bucketName]['contact_restriction']]);

                    SegmentStore.sanitizeSegmentRestriction([BucketMap[bucketName]['account_restriction']]);
                    SegmentStore.sanitizeSegmentRestriction([BucketMap[bucketName]['contact_restriction']]);
                });

                $timeout(function () {
                    vm.getRatingsAndRecordCounts(RatingEngineCopy, CurrentRatingEngine.segment.name);
                }, 250);
            } else {
                QueryStore.setEntitiesProperty('loading', true);
                $timeout(function () {
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

                    QueryStore.getEntitiesCounts(SegmentStore.sanitizeSegment(segment)).then(function (result) {
                        QueryStore.setResourceTypeCount('accounts', false, result['Account']);
                        QueryStore.setResourceTypeCount('contacts', false, result['Contact']);
                    });
                }, 250);
            }
        }

        vm.getRuleRecordCounts = function (restrictions) {
            var restrictions = restrictions || vm.getAllBucketRestrictions(),
                segmentId = CurrentRatingEngine.segment.name;

            vm.RuleRecordMap = {};

            restrictions.forEach(function (bucket, index) {
                bucket.bucketRestriction.bkt.Cnt = -1;

                vm.RuleRecordMap[bucket.bucketRestriction.attr + '_' + index] = bucket;
            })

            return RatingsEngineStore.getBucketRuleCounts(angular.copy(restrictions), segmentId);
        }

        vm.getAllBucketRestrictions = function () {
            var RatingEngineCopy = RatingEngineModel,
                BucketMap = RatingEngineCopy.rule.ratingRule.bucketToRuleMap,
                restrictions = [];

            vm.bucketLabels.forEach(function (bucketName, index) {
                var accountRestriction = BucketMap[bucketName]['account_restriction'];
                var accountLogical = { logicalRestriction: { operator: 'AND', restrictions: [] } };
                if (accountRestriction && accountRestriction != null) {
                    accountLogical = BucketMap[bucketName]['account_restriction'].logicalRestriction;
                } else {
                    accountLogical = { operator: 'AND', restrictions: [] };
                    BucketMap[bucketName]['account_restriction'] = { logicalRestriction: accountLogical };
                }

                var contactRestriction = BucketMap[bucketName]['contact_restriction'];
                var contactLogical = { logicalRestriction: { operator: 'AND', restrictions: [] } };
                if (contactRestriction && contactRestriction != null) {
                    contactLogical = BucketMap[bucketName]['contact_restriction'].logicalRestriction;
                } else {
                    contactLogical = { operator: 'AND', restrictions: [] };
                    BucketMap[bucketName]['contact_restriction'] = { logicalRestriction: contactLogical };
                }

                QueryStore.getAllBuckets(accountLogical.restrictions, restrictions);
                QueryStore.getAllBuckets(contactLogical.restrictions, restrictions);
            });

            return restrictions;
        }

        vm.saveSegment = function () {
            var segment = QueryStore.getSegment(),
                restriction = QueryStore.getAccountRestriction();

            vm.labelIncrementor = 0;
            vm.saving = true;

            SegmentStore.CreateOrUpdateSegment(segment, restriction).then(function (result) {
                vm.labelIncrementor = 0;
                vm.saving = false;
                vm.updateCount();
                vm.setCurrentSavedTree();
            });
        }

        vm.compareTree = function (old, current) {
            // remove AQB properties like labelGlyph/collapse
            SegmentStore.sanitizeSegmentRestriction(old);
            SegmentStore.sanitizeSegmentRestriction(angular.copy(current));

            return (JSON.stringify(old) === JSON.stringify(current));
        }

        vm.checkDisableSave = function () {
            // FIXME: this stuff is disabled for now
            if (!QueryStore.currentSavedTree || !vm.tree) {
                return true;
            }

            var old = angular.copy(QueryStore.currentSavedTree),
                current = angular.copy(vm.tree);

            return vm.compareTree(old, current);
        }

        vm.goAttributes = function () {
            if (vm.mode == 'rules') {
                var state = 'home.ratingsengine.rulesprospects.segment.attributes.add';
            } else if (vm.mode == 'dashboardrules') {
                var state = 'home.ratingsengine.dashboard.segment.attributes.add';
            } else {
                var state = vm.inModel
                    ? 'home.model.analysis.explorer.attributes'
                    : 'home.segment.explorer.attributes';
            }

            $state.go(state, {
                segment: $stateParams.segment
            });
        }

        vm.mouseUp = function (event) {
            var dragged = vm.draggedItem,
                dropped = vm.droppedItem;

            if (dragged && (!dropped || (dropped && dropped.uniqueId !== dragged.uniqueId))) {
                vm.droppedItem = vm;

                if (dropped) {
                    this.saveState();
                    vm.dropMoveItem(dragged, dropped);
                }
            }

            $timeout.cancel(vm.mouseDownTimer);
            vm.mouseDownTimer = false;

            vm.draggedItem = null;
            vm.droppedItem = null;

            if (vm.draggedClone) {
                vm.draggedClone.remove();
            }

            delete vm.droppedItemAppend;
            delete vm.draggedClone;
        }

        vm.dropMoveItem = function (dragged, dropped, endMove) {
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
                    var restrictions = dropped.tree.logicalRestriction.restrictions;

                    if (vm.droppedItemAppend) {
                        restrictions.push(draggedItem);
                    } else {
                        restrictions.splice(0, 0, draggedItem);
                    }
                } else {
                    var inc = vm.droppedItemAppend ? 1 : 0;

                    droppedParent.splice(droppedIndex + inc, 0, draggedItem);
                }

                draggedParent.splice(draggedParent.indexOf(dragged.tree), 1);
            }
        }

        vm.clickTreeMode = function (value) {
            vm.treeMode = value;

            vm.restriction = QueryStore[value + 'Restriction'];
            vm.tree = vm.getTree();

            vm.setCurrentSavedTree();
        }

        vm.categoryClass = function (category) {
            //console.log('[advanced]', category);
            var category = 'category-' + category.toLowerCase().replace(/\s/g, "-");
            return category;
        }

        vm.init();
    });