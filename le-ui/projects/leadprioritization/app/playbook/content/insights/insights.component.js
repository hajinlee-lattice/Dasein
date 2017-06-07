angular.module('lp.playbook.wizard.insights', [])
.controller('PlaybookWizardInsights', function(
    $scope, PlaybookWizardStore, CgTalkingPointStore, TalkingPointAttributes, TalkingPoints
) {
    var vm = this;

    angular.extend(vm, {
        attributes: TalkingPointAttributes,
        talkingPoints: TalkingPoints
    });

    vm.addTalkingPoint = function() {
        vm.talkingPoints.push({timestamp: new Date().getTime(), Title: null, Content: null, Offset: vm.talkingPoints.length, IsNew: true});
    };

    vm.onDelete = function(pos) {
    vm.talkingPoints.splice(pos, 1);
        for (var i = pos; i < vm.talkingPoints.length; i++) {
            vm.talkingPoints[i].Offset--;
        }
    };

    vm.reorder = function(from, to) {
        var tmp = vm.talkingPoints[from];
        vm.talkingPoints[from] = vm.talkingPoints[to];
        vm.talkingPoints[to] = tmp;
        vm.talkingPoints.forEach(function(tp, i) {
            tp.Offset = i;
        });
    };

    function validateTalkingPoints() {
        for (var i = 0; i < vm.talkingPoints.length; i++) {
            if (!vm.talkingPoints[i].Content || !vm.talkingPoints[i].Title) {
                PlaybookWizardStore.setValidation('insights', false);
                return false;
            }
        }

        PlaybookWizardStore.setValidation('insights', true);
        return true;
    };

    $scope.$watch(validateTalkingPoints);
});
