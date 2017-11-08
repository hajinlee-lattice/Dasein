angular.module('lp.ratingsengine.ai.refine',[])
.controller('RatingsEngineAIRefine', function($scope){
    var vm = this;

    angular.extend(vm,{
        test: true
    });

    vm.init = function(){
        console.log('Init refine');
    }

    vm.init();
});