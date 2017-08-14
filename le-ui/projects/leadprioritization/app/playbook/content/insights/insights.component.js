angular.module('lp.playbook.wizard.insights', [])
.controller('PlaybookWizardInsights', function(
    $scope, $state, $stateParams, $document, $rootScope, $timeout,
    PlaybookWizardStore, CgTalkingPointStore, TalkingPointPreviewResources, TalkingPointAttributes, TalkingPoints, BrowserStorageUtility
) {
    var vm = this;

    angular.extend(vm, {
        previewResources: TalkingPointPreviewResources,
        attributes: TalkingPointAttributes,
        talkingPoints: TalkingPoints,
        saveOnBlur: CgTalkingPointStore.saveOnBlur,
        stateParams: $stateParams,
        revertClicked: false,
        saving: false,
        saved: false
    });

    $rootScope.$on('talkingPoints:sync', function(e){
        CgTalkingPointStore.getTalkingPoints($stateParams.play_name, true).then(function(talkingPoints) {
            vm.talkingPoints = talkingPoints;
            $rootScope.$broadcast('talkingPoints:sync:complete');
        });
    });

    var savedTimeout;
    $rootScope.$on('talkingPoints:saving', function(e){
        vm.saving = true;
        vm.saved = false;
        $timeout.cancel(savedTimeout);
    });
    $rootScope.$on('talkingPoints:saved', function(e){
        vm.saving = false;
        vm.saved = true;
        $timeout.cancel(savedTimeout);
        savedTimeout = $timeout(function(){
            vm.saving = false;
            vm.saved = false;
        }, 5*1000);
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
                offset: vm.talkingPoints.length
            });
        talkingPoint.IsNew = true;
        vm.talkingPoints.push(talkingPoint);
        CgTalkingPointStore.setEditedTalkingPoint(talkingPoint);
    };

    vm.saveTalkingPoints = function() {
        CgTalkingPointStore.saveTalkingPoints(vm.talkingPoints).then(function(results){
            CgTalkingPointStore.getTalkingPoints($stateParams.play_name, true).then(function(talkingPoints) {
                $state.go('home.playbook.dashboard.insights.preview', {play_name: $stateParams.play_name});
            });
        });
    }

    vm.onDelete = function(pos) {
        var remove_talkingpoint_name = vm.talkingPoints[pos].name;
        if(vm.talkingPoints[pos].pid) {
            CgTalkingPointStore.deleteTalkingPoint(remove_talkingpoint_name).then(function(response){
            });
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
        if(CgTalkingPointStore.saveOnBlur) {
            CgTalkingPointStore.saveTalkingPoints(vm.talkingPoints).then(function(results){
                CgTalkingPointStore.getTalkingPoints($stateParams.play_name, true);
            });
        }
        
    };

    vm.revertTalkingPoints = function() {
        CgTalkingPointStore.revertTalkingPoints($stateParams.play_name).then(function(response){
            CgTalkingPointStore.getTalkingPoints($stateParams.play_name, true).then(function(talkingPoints) {
                vm.talkingPoints = talkingPoints;
            });
        });
    }

    function validateTalkingPoints() {
        var valid = false,
            errors = 0;
        if(vm.talkingPoints.length) {
            for (var i = 0; i < vm.talkingPoints.length; i++) {
                vm.talkingPoints[i].uiError = null;
                if (!vm.talkingPoints[i].content || !vm.talkingPoints[i].title) {
                    vm.talkingPoints[i].uiError = {};
                    if(!vm.talkingPoints[i].title) {
                        vm.talkingPoints[i].uiError.title = 'Please provide a title.';
                    }
                    if(!vm.talkingPoints[i].content) {tpeditor.component.html
                        vm.talkingPoints[i].uiError.content = 'Please provide content.';
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

    vm.revertClick = function($event, val) {
        $event.stopPropagation();

        vm.revertClicked = val;
        if (val) {
            $document.on('click', handleDocumentClick);
        } else {
            $document.off('click', handleDocumentClick);
        }
    };
    
    function handleDocumentClick(evt) {
        if (vm.revertClicked) {
            vm.revertClicked = false;
            $document.off('click', handleDocumentClick);
            $scope.$digest();
        }
    }

    $scope.$on('$destroy', function() {
        $document.off('click', handleDocumentClick);
    });

    $scope.$watch(validateTalkingPoints);
});
