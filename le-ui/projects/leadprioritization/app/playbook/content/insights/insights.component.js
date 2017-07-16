angular.module('lp.playbook.wizard.insights', [])
.controller('PlaybookWizardInsights', function(
    $scope, $stateParams, PlaybookWizardStore, CgTalkingPointStore, TalkingPointPreviewResources, TalkingPointAttributes, TalkingPoints, BrowserStorageUtility
) {
    var vm = this;

    angular.extend(vm, {
        previewResources: TalkingPointPreviewResources,
        attributes: TalkingPointAttributes,
        talkingPoints: TalkingPoints
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
        console.log(vm.talkingPoints);
        CgTalkingPointStore.saveTalkingPoints(vm.talkingPoints);
    }

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
        console.log(vm.talkingPoints);
        var valid = false;
        for (var i = 0; i < vm.talkingPoints.length; i++) {
            if (!vm.talkingPoints[i].content || !vm.talkingPoints[i].title) {
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
