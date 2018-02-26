angular.module('lp.ratingsengine.wizard.prioritization', [])
    .controller('RatingsEngineAIPrioritization', function ($scope, $stateParams, RatingsEngineStore, RatingsEngineService, Products) {
        var vm = this;
        // angular.extend(vm, {

        // });

        vm.init = function () {
            if($stateParams.rating_id) {
                RatingsEngineStore.getRating($stateParams.rating_id).then(function(rating){
                    console.log(rating);
                    vm.predictionType = rating.activeModel.AI.predictionType;
                    vm.validateNextStep();
                });
            }
        }

        vm.setPredictionType = function(predictionType){
            RatingsEngineStore.setPredictionType(predictionType);
            vm.predictionType = predictionType;
            vm.validateNextStep();
        }

        vm.validateNextStep = function () {
            if (RatingsEngineStore.getPredictionType() != null) {
                vm.setValidation('prioritization', true);
            } else {
                vm.setValidation('prioritization', false);
            }
        }
        vm.setValidation = function (type, validated) {
            RatingsEngineStore.setValidation(type, validated);
        }

        vm.init();
    });