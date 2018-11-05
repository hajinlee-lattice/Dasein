angular.module('lp.cg.talkingpoint.editor', [])
.directive('cgTalkingPointEditor', function() {
    return {
        restrict: 'E',
        replace: true,
        scope: {
            onDelete: '&',
            tp: '='
        },
        templateUrl: 'app/cgtalkingpoint/tpeditor/tpeditor.component.html',
        controller: 'CgTalkingPointEditorController',
        controllerAs: 'vm'
    }
})
.service('CgTinyMceConfig', function($stateParams, $rootScope, $timeout, CgTalkingPointStore) {
    this.config = {
        branding: false,
        plugins: 'textcolor lists link table legacyoutput paste code',
        menubar: false,
        toolbar: 'undo redo | bold italic underline | alignleft aligncenter alignright alignjustify | bullist fontsizeselect forecolor link unlink | pastetext code | table',
        elementpath: false,
        resize: true,
        height: 250,
        paste_auto_cleanup_on_paste : true,
        paste_text_linebreaktype: 'p',
        paste_retain_style_properties: 'font-size',
        forced_root_block: 'p',
        convert_newlines_to_brs: true,
        element_format : 'html',
        preformatted : true,
        convert_fonts_to_spans: false,
        default_link_target: '_blank',
        target_list: false,
        setup : function(ed) {
            ed.on('keydown', function (e, evt) {
                // prevents tabbing while in the wysiwyg
                if (e.keyCode == 9) {
                    e.preventDefault()
                }
            });
            ed.on('focus', function(e) {
                var textarea = angular.element(tinymce.activeEditor.getElement()),
                    name = textarea.data('name'),
                    talkingPoint = CgTalkingPointStore.getTalkingPoint(name);

                CgTalkingPointStore.setEditedTalkingPoint(talkingPoint);
            });
            ed.on('blur', function(e) {
                $timeout(function(){
                    if(!CgTalkingPointStore.saveOnBlur || CgTalkingPointStore.deleteClicked) {
                        return false;
                    }
                    var talkingPoint = CgTalkingPointStore.getEditedTalkingPoint(),
                        elementName = angular.element(e.target.contentAreaContainer).closest('.tinymce-container').data('name');

                    if(elementName !== talkingPoint.name) {
                        return false;
                    }
                    var content = tinymce.activeEditor.getContent();

                    if(content === '') {
                        return false;
                    }

                    talkingPoint.content = content;

                    if(CgTalkingPointStore.isTalkingPointDirty(talkingPoint) && !CgTalkingPointStore.getSavingFlag()) {
                        $rootScope.$broadcast('talkingPoints:lock', true);
                        CgTalkingPointStore.saveTalkingPoints([talkingPoint]).then(function(results){
                            if(talkingPoint.IsNew) {
                                $rootScope.$broadcast('talkingPoints:sync');
                            }
                            $rootScope.$broadcast('talkingPoints:lock', false);
                        });
                    }
                }, 100);
            });
        }
    };
})
.controller('CgTalkingPointEditorController', function($scope, $stateParams, $rootScope, $element, $document, $timeout, $q, CgTinyMceConfig, CgTalkingPointStore) {
    var vm = this;
    angular.extend(this, {
        tinyMceConfig: CgTinyMceConfig.config,
        expanded: false,
        deleteClicked: false,
        lockTalkingPoints: false
    });

    if ($scope.tp.IsNew === true || !($scope.tp.title && $scope.tp.content) || Date.now() - $scope.tp.created < 1000) { // if it hasn't been saved
        vm.expanded = true;
    }

    vm.CgTalkingPointStore = CgTalkingPointStore;

    vm.expand = function(bool) {
        vm.expanded = (bool ? bool : !vm.expanded);
        $timeout(function(){
            if(vm.expanded) {
                CgTalkingPointStore.setEditedTalkingPoint($scope.tp);
            }
        }, 100);
    };

    vm.titleFocus = function(talkingPoint) {
        $timeout(function(){
            CgTalkingPointStore.setEditedTalkingPoint(talkingPoint);
        }, 100);
    }

    if((Date.now() - $scope.tp.updated) < 2000) { // if it has been saved but you want to re-open it
        vm.expand(true);
    }

    vm.deleteClick = function($event, val) {
        $event.stopPropagation();

        vm.deleteClicked = val;
        CgTalkingPointStore.deleteClicked = val;
        if (val) {
            $document.on('click', handleDocumentClick);
        } else {
            $document.off('click', handleDocumentClick);
            vm.saveTitle();
        }
    };

    vm.setDeleteClicked = function(bool) {
        vm.deleteClicked = bool;
        CgTalkingPointStore.deleteClicked = bool;
        $rootScope.$broadcast("deleteClicked");
    }

    $rootScope.$on('talkingPoints:lock', function(e, bool){
        vm.lockTalkingPoints = bool;
    });

    vm.saveTitle = function() {
        $timeout(function(){
            if(!CgTalkingPointStore.saveOnBlur || vm.deleteClicked || !$scope.tp.title) {
                return false;
            }
            CgTalkingPointStore.setEditedTalkingPoint($scope.tp, 'title');
            if(CgTalkingPointStore.isTalkingPointDirty($scope.tp)) {
                vm.lockTalkingPoints = true;
                CgTalkingPointStore.saveTalkingPoints([$scope.tp]).then(function(data){
                    if($scope.tp.IsNew) {
                        $rootScope.$broadcast('talkingPoints:sync');
                        $rootScope.$on('talkingPoints:sync:complete', function(e){
                            vm.lockTalkingPoints = false;
                        });
                    } else {
                        vm.lockTalkingPoints = false;
                    }
                });
            };
        },100);
    }

    function handleDocumentClick(evt) {
        if (vm.deleteClicked) {
            vm.deleteClicked = false;
            CgTalkingPointStore.deleteClicked = false;
            $document.off('click', handleDocumentClick);
            $scope.$digest();
        }
    }

    $scope.$on('$destroy', function() {
        $document.off('click', handleDocumentClick);
    });

    $scope.$watch('tp.offset', function(a,b) {
        if (a !== b) {
            var wasExpanded = vm.expanded;
            vm.expanded = false;

            broadcastRefresh().then(function() {
                vm.expanded = wasExpanded;
            });
        }
    });

    function broadcastRefresh() {
        var deferred = $q.defer();
        deferred.resolve($scope.$broadcast('$tinymce:refresh'));
        return deferred.promise;
    }
});
