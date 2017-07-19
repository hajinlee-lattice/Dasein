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
.service('CgTinyMceConfig', function() {
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
        target_list: false
    };
})
.controller('CgTalkingPointEditorController', function($scope, $element, $document, $timeout, $q, CgTinyMceConfig) {
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
        vm.expanded = !vm.expanded;
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
