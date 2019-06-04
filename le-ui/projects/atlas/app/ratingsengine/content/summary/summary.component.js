angular.module('lp.ratingsengine.wizard.summary', [])
.controller('RatingsEngineSummary', function (
    $q, $state, $stateParams, Rating, CurrentRatingEngine, RatingsEngineModels, RatingsEngineStore, QueryStore
) {
    var vm = this;

    angular.extend(vm, {
        rating: Rating,
        labelIncrementor: 0,
        bucket: 'A',
        buckets: [],
        bucketsMap: {'A':0,'B':1,'C':2,'D':3,'E':4,'F':5},
        bucketLabels: [ 'A', 'B', 'C', 'D', 'E', 'F' ],
        default_bucket: 'A',
        rating_rule: {},
        coverage_map: {},
        rating_id: $stateParams.rating_id,
        ratings: RatingsEngineStore.ratings,
        treeMode: 'account',
        ratingStatus: false
    });

    vm.init = function() {

    	vm.rating_rule = RatingsEngineModels.rule.ratingRule;
        vm.rating_buckets = vm.rating_rule.bucketToRuleMap;
        vm.default_bucket = vm.rating_rule.defaultBucketName;

        RatingsEngineStore.setRule(RatingsEngineModels)

        vm.initCoverageMap();

        RatingsEngineStore.getCoverageMap(RatingsEngineModels, CurrentRatingEngine.segment.name).then(function(result) {
            var CoverageMap = vm.initCoverageMap(result);
            // console.log('[AQB] CoverageMap:', CoverageMap);
        }); 

        vm.getRuleRecordCounts();

        if(vm.rating.status = 'ACTIVE') {
        	vm.ratingStatus = true;
        } else {
        	vm.ratingStatus = false;
        }
        vm.rating.displayName = vm.rating.displayName.substring(0,50);
        vm.isValid = true;
    };

    vm.changeDetails = function(){
    	
        if(vm.rating.displayName.length === 0) {
            RatingsEngineStore.validation.summary = false;
        } else {
            RatingsEngineStore.validation.summary = true;
        };

    	if(vm.ratingStatus === true) {
    		vm.rating.status = 'ACTIVE';
    	} else {
    		vm.rating.status = 'INACTIVE';
    	}

        //TODO: Check why we have this set of the opts
    	// $stateParams.opts = {
    	// 	rating_id: $stateParams.rating_id,
    	// 	displayName: vm.rating.displayName,
    	// 	note: vm.rating.note,
    	// 	status: vm.rating.status
        // }
        // console.log('$stateParams ====> ', $stateParams);
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
            
            if(vm.coverage_map.bucketCoverageCounts) {
                vm.coverage_map.bucketCoverageCounts.forEach(function(bkt) {
                    vm.buckets[vm.bucketsMap[bkt.bucket]].count = bkt.count;
                });
            }
        }

        return map;
    }

    vm.getRuleCount = function(bkt) {
        return QueryStore.getRuleCount(bkt, vm.rating_rule.bucketToRuleMap, vm.bucketLabels);
    }

    vm.getRuleRecordCounts = function(restrictions) {
        var restrictions = restrictions || vm.getAllBucketRestrictions(),
            segmentId = CurrentRatingEngine.segment.name,
            map = {};

        restrictions.forEach(function(bucket, index) {
            bucket.bucketRestriction.bkt.Cnt = -1;

            map[bucket.bucketRestriction.attr + '_' + index] = bucket;
        })

        return RatingsEngineStore.getBucketRuleCounts(restrictions, segmentId);

        // RatingsEngineStore.getBucketRuleCounts(restrictions, segmentId).then(function(result) {
        //     var buckets = result.segmentIdAndSingleRulesCoverageMap;
            
        //     Object.keys(buckets).forEach(function(key) {
        //         var label = map[key].bucketRestriction.attr,
        //             type = label.split('.')[0] == 'Contact' ? 'contact' : 'account';
                
        //         map[key].bucketRestriction.bkt.Cnt = buckets[key][type + 'Count'];
        //     });
        // }); 
    }

    vm.getAllBucketRestrictions = function() {
        var RatingEngineCopy = RatingsEngineModels,
            BucketMap = RatingEngineCopy.rule.ratingRule.bucketToRuleMap,
            restrictions = [];

        var recursive = function(tree, restrictions) {
            if (!restrictions) {
                var restrictions = [];
            }

            tree.forEach(function(branch) {
                if (branch && branch.bucketRestriction && branch.bucketRestriction && branch.bucketRestriction.bkt.Id) {
                    restrictions.push(branch);
                }

                if (branch && branch.logicalRestriction) {
                    recursive(branch.logicalRestriction.restrictions, restrictions);
                }
            });
        };

        vm.bucketLabels.forEach(function(bucketName, index) {
            var logical = BucketMap[bucketName][vm.treeMode + '_restriction'].logicalRestriction;

            recursive(logical.restrictions, restrictions);
        });

        return restrictions;
    }


    vm.init();

});