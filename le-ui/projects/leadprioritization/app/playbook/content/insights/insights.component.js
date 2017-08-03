angular.module('lp.playbook.wizard.insights', [])
.controller('PlaybookWizardInsights', function(
    $scope, $state, $stateParams, $document,
    PlaybookWizardStore, CgTalkingPointStore, TalkingPointPreviewResources, TalkingPointAttributes, TalkingPoints, BrowserStorageUtility
) {
    var vm = this;

    angular.extend(vm, {
        previewResources: TalkingPointPreviewResources,
        attributes: TalkingPointAttributes,
        talkingPoints: TalkingPoints,
        stateParams: $stateParams
    });

    CgTalkingPointStore.getTalkingPoints($stateParams.play_name, true).then(function(talkingPoints) {
        vm.talkingPoints = talkingPoints;
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
        CgTalkingPointStore.setEditedTalkingPoint(talkingPoint);
    };

    vm.saveTalkingPoints = function() {
        // I was going to check to confirm there was a change first but offset always changes, so you can't compare ojects as it currently is so just always save
        CgTalkingPointStore.saveTalkingPoints(vm.talkingPoints).then(function(results){
            CgTalkingPointStore.getTalkingPoints($stateParams.play_name, true).then(function(talkingPoints) {
                $state.go('home.playbook.dashboard.insights.preview', {play_name: $stateParams.play_name});
            });
        });
    }

    vm.onDelete = function(pos) {
        var remove_talkingpoint_name = vm.talkingPoints[pos].name;
        if(vm.talkingPoints[pos].pid) {
            CgTalkingPointStore.deleteTalkingPoint(remove_talkingpoint_name);
        }
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
        CgTalkingPointStore.saveTalkingPoints(vm.talkingPoints).then(function(results){
            CgTalkingPointStore.getTalkingPoints($stateParams.play_name, true);
        });
        
    };

    function validateTalkingPoints() {
        var valid = false,
            errors = 0;
        if(vm.talkingPoints.length) {
            for (var i = 0; i < vm.talkingPoints.length; i++) {
                vm.talkingPoints[i].uiError = null;
                if (!vm.talkingPoints[i].content || !vm.talkingPoints[i].title) {
                    vm.talkingPoints[i].uiError = {};
                    if(!vm.talkingPoints[i].title) {
                        vm.talkingPoints[i].uiError.title = 'Missing Title';
                    }
                    if(!vm.talkingPoints[i].content) {
                        vm.talkingPoints[i].uiError.content = 'Missing Content';
                    }
                    errors++;
                }
            }
            if (errors) {
                PlaybookWizardStore.setValidation('insights', false);
                valid = false;
            } else {
                valid = true;
            }
        } else {
            valid = false;
        }
        //valid = true;
        vm.valid = valid;
        if(valid) {
            PlaybookWizardStore.setTalkingPoints(vm.talkingPoints);
            PlaybookWizardStore.setValidation('insights', true);
        }
        return true;
    };

    $scope.$watch(validateTalkingPoints);
});
