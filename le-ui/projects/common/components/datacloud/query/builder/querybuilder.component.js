angular.module('common.datacloud.querybuilder', [])
.controller('QueryBuilderCtrl', function($scope, $state, QueryRestriction, QueryStore, SegmentServiceProxy) {

    var vm = this;
    angular.extend(this, {
        AnyAttributes: QueryRestriction.any,
        AllAttributes: QueryRestriction.all
    });

    vm.update = function () {
        // debounced, spinner, update counts, update data
    };

    vm.moveToAll = function(index) {
        move(vm.AnyAttributes, vm.AllAttributes, index);
    };

    vm.moveToAny = function(index) {
        move(vm.AllAttributes, vm.AnyAttributes, index);
    };

    function move(src, dest, index) {
        var item = src[index];
        if (item) {
            src.splice(index, 1);
            dest.push(item);
        }
    }

    vm.delete = function(src, index) {
        if (index < src.length) {
            src.splice(index, 1);
        }
    };

    vm.goAttributes = function() {
        $state.go('home.model.analysis.explorer.attributes');
    };

    vm.saveSegment = function () {
        SegmentServiceProxy.CreateOrUpdateSegment().then(function(result) {
            if (result.errorMsg) {
            } else {
                $state.go('home.model.segmentation', {}, {notify: true})
            }
        });
    }
});
