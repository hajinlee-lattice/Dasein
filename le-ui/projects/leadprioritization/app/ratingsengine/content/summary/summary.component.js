angular.module('lp.ratingsengine.wizard.summary', [])
.controller('RatingsEngineSummary', function ($q, $state, $stateParams, Rating, CoverageMap, RatingsEngineStore) {
    var vm = this;

    angular.extend(vm, {
        rating: Rating,
        CoverageMap: CoverageMap
    });

    vm.init = function() {

    	console.log(vm.rating, vm.CoverageMap);

    	var segmentId = vm.rating.segment.name;
    	vm.coverage_map = vm.CoverageMap.segmentIdModelRulesCoverageMap[segmentId];
    	vm.buckets = vm.CoverageMap.segmentIdModelRulesCoverageMap[segmentId].bucketCoverageCounts;

    	console.log(vm.rating, vm.buckets, vm.Cover);

    };


    // vm.getRuleCount = function(bkt) {
    //     if (bkt) {
    //         var buckets = [
    //             vm.rating_rule.bucketToRuleMap[bkt.bucket] 
    //         ];
    //     } else {
    //         var buckets = [ 
    //             vm.rating_rule.bucketToRuleMap['A'], 
    //             vm.rating_rule.bucketToRuleMap['A-'], 
    //             vm.rating_rule.bucketToRuleMap['B'], 
    //             vm.rating_rule.bucketToRuleMap['C'], 
    //             vm.rating_rule.bucketToRuleMap['D'], 
    //             vm.rating_rule.bucketToRuleMap['F'] 
    //         ];
    //     }

    //     var filtered = [];

    //     buckets.forEach(function(bucket, index) {
    //         var restrictions = bucket[vm.treeMode + '_restriction'].logicalRestriction.restrictions;
            
    //         filtered = filtered.concat(restrictions.filter(function(value, index) {
    //             return value.bucketRestriction.bkt && value.bucketRestriction.bkt.Id;
    //         }));
    //     })

    //     return filtered.length;
    // }


    vm.init();

});