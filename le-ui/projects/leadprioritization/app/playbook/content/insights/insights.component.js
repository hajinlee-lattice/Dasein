angular.module('lp.playbook.wizard.insights', [])
.controller('PlaybookWizardInsights', function(
    $scope, $stateParams, PlaybookWizardStore, CgTalkingPointStore, TalkingPointAttributes, TalkingPoints, BrowserStorageUtility
) {
    var vm = this;

    angular.extend(vm, {
        attributes: TalkingPointAttributes,
        talkingPoints: TalkingPoints
    });

    vm.addTalkingPoint = function() {
        vm.talkingPoints.push({
            timestamp: new Date().getTime(), 
            Title: null, 
            Content: null, 
            Offset: vm.talkingPoints.length, 
            IsNew: true, 
            playExternalID: $stateParams.play_name,
            customerID: BrowserStorageUtility.getClientSession().Tenant.Identifier
        });
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
        var talkingPoints = [];
        for (var i = 0; i < vm.talkingPoints.length; i++) {
            if (!vm.talkingPoints[i].Content || !vm.talkingPoints[i].Title) {
                PlaybookWizardStore.setValidation('insights', false);
                return false;
            }
            var talkingPoint = CgTalkingPointStore.generateTalkingPoint(vm.talkingPoints[i]);
            talkingPoints.push(talkingPoint);
        }
        PlaybookWizardStore.setTalkingPoints(talkingPoints);

        PlaybookWizardStore.setValidation('insights', true);
        return true;
    };

    $scope.$watch(validateTalkingPoints);
});
