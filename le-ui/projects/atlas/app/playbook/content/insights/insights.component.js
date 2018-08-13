angular.module('lp.playbook.wizard.insights', [])
.controller('PlaybookWizardInsights', function(
    $scope, $state, $stateParams, $document, $rootScope, $timeout,
    PlaybookWizardStore, CgTalkingPointStore, TalkingPointPreviewResources, 
    TalkingPointAttributes, Entities, 
    TalkingPoints, BrowserStorageUtility
) {
    var vm = this;

    angular.extend(vm, {
        previewResources: TalkingPointPreviewResources,
        attributes: TalkingPointAttributes,
        entities: Entities,
        talkingPoints: TalkingPoints,
        saveOnBlur: CgTalkingPointStore.saveOnBlur,
        stateParams: $stateParams,
        revertClicked: false,
        saving: false,
        saved: false,
        currentPlay: PlaybookWizardStore.getCurrentPlay()
    });

    vm.init = function() {
        if($stateParams.play_name) {
            PlaybookWizardStore.getPlay($stateParams.play_name);
        }
    }

    vm.init();

    $rootScope.$on('talkingPoints:sync', function(e){
        CgTalkingPointStore.getTalkingPoints($stateParams.play_name, true).then(function(talkingPoints) {
            var _tp = [];
            talkingPoints.forEach(function(talkingPoint, index) {
                delete talkingPoint.title;
                delete talkingPoint.content;
                if(vm.talkingPoints && vm.talkingPoints[index]) {
                    delete vm.talkingPoints[index].pid;
                    delete vm.talkingPoints[index].name;
                    delete vm.talkingPoints[index].IsNew;
                }
                _tp.push(angular.extend({}, talkingPoint, vm.talkingPoints[index]));
            });
            vm.talkingPoints = _tp;

            //vm.talkingPoints = talkingPoints;

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

    var newTalkingPoint = function() {
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
        CgTalkingPointStore.saveTalkingPoints(vm.talkingPoints).then(function(results){
            vm.talkingPoints = results;
            CgTalkingPointStore.setEditedTalkingPoint(results.slice(-1)[0]);
        });
    }

    vm.addTalkingPoint = function() {
        newTalkingPoint();
    };

    vm.saveTalkingPoints = function() {
        CgTalkingPointStore.saveTalkingPoints(vm.talkingPoints).then(function(results){
            CgTalkingPointStore.getTalkingPoints($stateParams.play_name, true).then(function(talkingPoints) {
                $state.go('home.playbook.dashboard.insights.preview', {play_name: $stateParams.play_name});
            });
        });
    }

    vm.onDelete = function(talkingPoint, pos) {
        if(!talkingPoint.IsNew) {
            CgTalkingPointStore.deleteTalkingPoint(talkingPoint.name);
        }
        vm.talkingPoints.splice(pos, 1);
        for (var i = pos; i < vm.talkingPoints.length; i++) {
            vm.talkingPoints[i].Offset--;
        }
        validateTalkingPoints();
    };

    var reorderTalkingPoints = function(from, to) {
        var min = Math.min(from, to),
            talkingPoints = sortBy('offset', vm.talkingPoints),
            ret = [],
            top = talkingPoints.slice(0, min + 1),
            bottom = talkingPoints.slice(min + 1, talkingPoints.length),
            topOffset = top[top.length - 1].offset,
            bottomOffset = bottom[0].offset;

        top[top.length - 1].offset = bottomOffset;
        bottom[0].offset = topOffset;
        ret = sortBy('offset', top.concat(bottom));

        return ret;
    }

    var sortBy = function(property, obj) { 
        var arr = obj.slice(0);
        arr.sort(function(a,b) {
            return a[property] - b[property];
        });
        return arr;
    }

    vm.reorder = function(from, to) {
        vm.talkingPoints = reorderTalkingPoints(from, to);
        
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
                    if(!vm.talkingPoints[i].content) {
                        vm.talkingPoints[i].uiError.content = 'Please provide content.';
                    }
                    errors++;
                }
            }
            if (errors) {
                //PlaybookWizardStore.setValidation('insights', false);
                valid = false;
            } else {
                valid = true;
            }
        } else {
            valid = false;
        }
        vm.valid = valid;
        if(valid) {
            PlaybookWizardStore.setTalkingPoints(vm.talkingPoints);
        }
        PlaybookWizardStore.setValidation('insights', true);
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

    vm.allowAddTalkingPoint = function(talkingPoints, valid) {
        if(!talkingPoints.length) {
            return true;
        }
        if(valid) {
            return true;
        }
        return false;
    }
    
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
