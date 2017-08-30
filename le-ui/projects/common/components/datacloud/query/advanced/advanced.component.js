angular.module('common.datacloud.query.advanced', [
    'common.datacloud.query.advanced.input',
    'common.datacloud.query.advanced.tree'
])
.controller('AdvancedQueryCtrl', function($scope, $state, $stateParams, $timeout, $q,
    QueryRestriction, QueryStore, DataCloudStore, SegmentStore, QueryService, Cube,
    BucketRestriction, CurrentConfiguration, BrowserStorageUtility, QueryStore
) {
    var vm = this;

    angular.extend(this, {
        inModel: $state.current.name.split('.')[1] === 'model',
        state: $stateParams.state,
        items: [],
        enrichments: [],
        enrichmentsMap: DataCloudStore.getEnrichmentsMap(),
        restriction: QueryStore.restriction,
        cube: Cube,
        labelIncrementor: 0,
        history: QueryStore.history
    });

    vm.init = function() {
        vm.getTree();

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
                console.log('-!- restrictions:', angular.copy(vm.restriction))
                console.log('-!- items:', vm.items)
                console.log('-!- cube:', vm.cube)
            },1);
        });

        if (!QueryStore.currentSavedTree) {
            vm.setCurrentSavedTree();
        }
    }

    vm.getTree = function() {
        vm.tree = [ 
            vm.restriction.restriction.logicalRestriction.restrictions[0] 
        ];
    }

    vm.pushItem = function(item, tree) {
        if (item) {
            var cube = vm.cube[item.ColumnId];

            item.cube = cube;
            item.topbkt = tree.bkt;
            //console.log(item, tree);


            vm.items.push(item);
        }
    }

    vm.setCurrentSavedTree = function() {
        QueryStore.currentSavedTree = angular.copy(vm.restriction.restriction.logicalRestriction.restrictions[0]);
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

        var tree = angular.copy(vm.tree),
            old = angular.copy(vm.history[vm.history.length -1]);

        // remove AQB properties like labelGlyph/collapse
        SegmentStore.sanitizeSegmentRestriction([tree]);
        SegmentStore.sanitizeSegmentRestriction([old]);

        if (JSON.stringify(old) !== JSON.stringify(tree)) {
            vm.history.push(tree);
            console.log('save', vm.history.length, vm.history, tree, old);

            if (!noCount) {
                vm.updateCount();
            }
        }
    }

    vm.clickUndo = function() {
        var lastState = vm.history.pop();

        if (lastState) {
            vm.restriction.restriction.logicalRestriction.restrictions[0] = lastState[0];
            vm.tree = lastState;
            vm.updateCount();
        }
    }

    vm.updateCount = function() {
        QueryStore.counts.accounts.loading = true;

        QueryService.GetCountByQuery('accounts', SegmentStore.sanitizeSegment({ 
            'free_form_text_search': "",
            'frontend_restriction': angular.copy(vm.restriction),
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
            "frontend_restriction": {
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
            restriction = QueryStore.getRestriction();

        vm.labelIncrementor = 0;
        vm.saving = true;

        SegmentStore.CreateOrUpdateSegment(segment, restriction).then(function(result) {
            vm.labelIncrementor = 0;
            vm.saving = false;
            vm.updateCount();
            vm.setCurrentSavedTree();
        });
    }

    vm.checkDisableSave = function() {
        var old = angular.copy(QueryStore.currentSavedTree),
            current = angular.copy(vm.tree[0]);

        // remove AQB properties like labelGlyph/collapse
        SegmentStore.sanitizeSegmentRestriction([old]);
        SegmentStore.sanitizeSegmentRestriction([current]);
        
        return (JSON.stringify(old) === JSON.stringify(current));
    }

    vm.goAttributes = function() {
        var state = vm.inModel
                ? 'home.model.analysis.explorer.attributes'
                : 'home.segment.explorer.attributes';

        $state.go(state);
    }

    vm.init();
});