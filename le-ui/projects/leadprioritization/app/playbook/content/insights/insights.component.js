angular.module('lp.playbook.wizard.insights', [])
.controller('PlaybookWizardInsights', function(
    $scope, $state, $stateParams, PlaybookWizardStore, CgTalkingPointStore, TalkingPointPreviewResources, TalkingPointAttributes, TalkingPoints, BrowserStorageUtility
) {
    var vm = this;

    angular.extend(vm, {
        previewResources: TalkingPointPreviewResources,
        attributes: TalkingPointAttributes,
        talkingPoints: TalkingPoints,
        stateParams: $stateParams
    });

    var cachedTalkingPoints = angular.copy(TalkingPoints);

    vm.addTalkingPoint = function() {
        var talkingPoint = CgTalkingPointStore.generateTalkingPoint({
                timestamp: new Date().getTime(), 
                customerID: BrowserStorageUtility.getClientSession().Tenant.Identifier,
                playExternalID: $stateParams.play_name,
                title: null, 
                content: null, 
                offset: vm.talkingPoints.length,
            });
        talkingPoint.IsNew = true;
        vm.talkingPoints.push(talkingPoint);
    };

    vm.saveTalkingPoints = function() {
        // I was going to check to confirm there was a change first but offset always changes, so you can't compare ojects as it currently is so just always save
        CgTalkingPointStore.saveTalkingPoints(vm.talkingPoints).then(function(results){
            $state.go('home.playbook.dashboard.insights.preview', {play_name: $stateParams.play_name});
        });
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
            tp.offset = i;
        });
    };

    function validateTalkingPoints() {
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
