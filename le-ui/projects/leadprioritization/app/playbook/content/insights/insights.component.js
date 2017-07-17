angular.module('lp.playbook.wizard.insights', [])
.controller('PlaybookWizardInsights', function(
    $scope, $stateParams, PlaybookWizardStore, CgTalkingPointStore, TalkingPointPreviewResources, TalkingPointAttributes, TalkingPoints, BrowserStorageUtility
) {
    var vm = this;

    angular.extend(vm, {
        previewResources: TalkingPointPreviewResources,
        attributes: TalkingPointAttributes,
        talkingPoints: TalkingPoints,
        stateParams: $stateParams
    });

    vm.addTalkingPoint = function() {
        var talkingPoint = CgTalkingPointStore.generateTalkingPoint({
                timestamp: new Date().getTime(), 
                customerID: BrowserStorageUtility.getClientSession().Tenant.Identifier,
                playExternalID: $stateParams.play_name,
                Title: null, 
                Content: null, 
                Offset: vm.talkingPoints.length,
            });
        talkingPoint.IsNew = true;
        vm.talkingPoints.push(talkingPoint);
    };

    vm.saveTalkingPoints = function() {
        CgTalkingPointStore.saveTalkingPoints(vm.talkingPoints);
    }

    vm.onDelete = function(pos) {
        var remove_talkingpoint_name = vm.talkingPoints[pos].name;
        CgTalkingPointStore.deleteTalkingPoint(remove_talkingpoint_name);
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
        var valid = false;
        for (var i = 0; i < vm.talkingPoints.length; i++) {
            if (!vm.talkingPoints[i].Content || !vm.talkingPoints[i].Title) {
                PlaybookWizardStore.setValidation('insights', false);
                valid = false;
                return false;
            }
            valid = true;
        }
        if(valid) {
            PlaybookWizardStore.setTalkingPoints(vm.talkingPoints);
            PlaybookWizardStore.setValidation('insights', true);
        }
        return true;
    };

    $scope.$watch(validateTalkingPoints);
});
