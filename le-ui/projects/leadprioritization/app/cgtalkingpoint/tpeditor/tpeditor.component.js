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
.service('CgTinyMceConfig', function($stateParams, CgTalkingPointStore) {
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
            ed.on('blur', function(e) {
                var talkingPoint = CgTalkingPointStore.getEditedTalkingPoint();
                    content = ed.contentDocument.body.innerHTML;
                CgTalkingPointStore.setEditedTalkingPoint(content, 'description');
                if(CgTalkingPointStore.isTalkingPointDirty(talkingPoint)) {
                    CgTalkingPointStore.saveTalkingPoints([talkingPoint]).then(function(results){
                        CgTalkingPointStore.getTalkingPoints($stateParams.play_name, true);
                    });
                }
            });
        }
    };
})
.controller('CgTalkingPointEditorController', function($scope, $stateParams, $element, $document, $timeout, $q, CgTinyMceConfig, CgTalkingPointStore) {
    var vm = this;
    angular.extend(this, {
        tinyMceConfig: CgTinyMceConfig.config,
        expanded: false,
        deleteClicked: false
    });

    if ($scope.tp.IsNew === true) {
        vm.expanded = true;
        //delete $scope.tp.IsNew;
    }

    vm.expand = function() {
        CgTalkingPointStore.setEditedTalkingPoint($scope.tp);
        vm.expanded = !vm.expanded;
        var tmce = angular.element('iframe');
        tmce.on('focus',function(){
            console.log(focused);
        });
    };

    vm.deleteClick = function($event, val) {
        $event.stopPropagation();

        vm.deleteClicked = val;
        if (val) {
            $document.on('click', handleDocumentClick);
        } else {
            $document.off('click', handleDocumentClick);
        }
    };

    vm.saveTitle = function() {
        CgTalkingPointStore.setEditedTalkingPoint($scope.tp, 'title');
        if(CgTalkingPointStore.isTalkingPointDirty($scope.tp)) {
            CgTalkingPointStore.saveTalkingPoints([$scope.tp]).then(function(results){
                CgTalkingPointStore.getTalkingPoints($stateParams.play_name, true);
            });
        };
    }

    function handleDocumentClick(evt) {
        if (vm.deleteClicked) {
            vm.deleteClicked = false;
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
