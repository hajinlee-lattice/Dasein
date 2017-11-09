angular.module('lp.ratingsengine.ai.refine', [])
    .controller('RatingsEngineAIRefineTarget', function ($scope, RefineService) {
        var vm = this;

        angular.extend(vm, {
            refine: RefineService.refineModel,
            sellOption: '',
            notyet: false,
            resell: false,

        });

        vm.init = function () {
            RefineService.reset();
            console.log('Init refine Target');
            $scope.$watch(function () {
                return RefineService.refineModel;
            },
            function (newVal, oldVal) {
                // alert("Inside watch");
                vm.refine = newVal;
                console.log('NEW ' + newVal + ' - OLD ' + oldVal);
            }, true);
        }

        vm.showRefineModel = function () {
            console.log('Change view');
            RefineService.changeValue();
        }

        vm.init();
    })
    .controller('RatingsEngineAIRefineModel', function ($scope, RefineService) {
        var vm = this;
        angular.extend(vm, {
            refine: RefineService.refineModel,
            spent: false,
            bought: false,
            historical: false,
            similarProducts: false,
            similarSegments: false
        });

        vm.init = function () {
            console.info('Init refine Model');
            $scope.$watch(function () {
                return RefineService.refineModel;
            },
            function (newVal, oldVal) {
                // alert("Inside watch");
                vm.refine =newVal;
                console.log('NEW ' + newVal + ' - OLD ' + oldVal);
            }, true);
        }

        vm.init();
    })
    .service('RefineService', function () {
        this.refineModel = false;

        this.reset = function(){
            this.refineModel = false;
        }

        this.changeValue = function () {
            this.refineModel = !this.refineModel;
        }
        this.getRefineModel = function () {
            return this.refineModel;
        }
    });