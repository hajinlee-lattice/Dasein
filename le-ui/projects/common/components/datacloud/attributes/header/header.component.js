angular.module("common.attributes.header", []).component("attrHeader", {
    templateUrl:
        "/components/datacloud/attributes/header/header.component.html",
    bindings: {
        tabs: "<"
    },
    controller: function($state, $stateParams, StateHistory) {
        var vm = this;

        vm.$onInit = function() {
            vm.params = $stateParams;
        };

        vm.click = function(category) {
            angular.element("div#subSummaryView").addClass("inactive-disabled");
        };

        vm.getTo = function() {
            return StateHistory.lastTo().name;
        };

        vm.getFrom = function() {
            return StateHistory.lastFrom().name;
        };

        vm.isActive = function(sref) {
            var x = $state.current.name == sref;
            var y = vm.getTo() == sref;
            var z = vm.getFrom() != sref || vm.getTo() == $state.current.name;

            return (x || y) && z;
        };
    }
});
