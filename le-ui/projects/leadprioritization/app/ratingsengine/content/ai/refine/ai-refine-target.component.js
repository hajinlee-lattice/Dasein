angular.module('lp.ratingsengine.ai.refine', [])
    .controller('RatingsEngineAIRefineTarget', function ($scope) {
        var vm = this;

        angular.extend(vm, {
            sellOption: '',
            notyet: false,
            resell: false
        });

        vm.init = function () {
            console.log('Init refine');
        }

        vm.init();
    })
    .controller('RatingsEngineAIRefineModel', function ($scope) {
        var vm = this;
        angular.extend(vm, {
            spent: false,
            bought: false,
            historical: false,
            similarProducts: false,
            similarSegments: false
        });

        vm.init = function () {
            console.info('Refine Model init');
        }

        vm.init();
    });