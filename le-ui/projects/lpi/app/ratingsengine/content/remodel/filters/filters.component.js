angular.module('lp.ratingsengine.remodel.filters', [])
.component('attrRemodelFilters', {
    templateUrl: 'app/ratingsengine/content/remodel/filters/filters.component.html',
    bindings: {
        filters: '< '
    },
    controller: function ($state, $stateParams, AttrConfigStore) {
        var vm = this;

        vm.$onInit = function() {};

    }
});