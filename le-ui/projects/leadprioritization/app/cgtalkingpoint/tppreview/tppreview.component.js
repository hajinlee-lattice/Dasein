angular.module('lp.cg.talkingpoint.preview', [])
.directive('cgTalkingPointPreview', function() {
    return {
        restrict: 'E',
        replace: true,
        scope: {},
        templateUrl: 'app/cgtalkingpoint/tppreview/tppreview.component.html',
        controller: 'cgTalkingPointPreviewCtrl',
        controllerAs: 'vm'
    };
})
.controller('cgTalkingPointPreviewCtrl', function ($scope, $sce, $element, CgTalkingPointStore) {
    var talkingPoints = CgTalkingPointStore.getTalkingPoints();
    var iframe = null;

    var vm = this;
    angular.extend(vm, {
        hasTalkingPoints: talkingPoints.length > 0,
        leadPreviewObject: null,
        accounts: null,
        selected: null,
        sceIframeSrc: null
    });

    vm.init = function() {
        iframe = $element.find('#tppreview_iframe')[0];
        window.addEventListener('message', handleLpiPreviewInit);

        vm.leadPreviewObject = CgTalkingPointStore.generateLeadPreviewObject();
        CgTalkingPointStore.getAccounts().then(function(accounts) {
            vm.accounts = accounts;

            return CgTalkingPointStore.getDanteUrl();
        }).then(function(danteUrl) {
            vm.sceIframeSrc = $sce.trustAsResourceUrl(danteUrl);
        }).then(function() {
            vm.selected = vm.accounts[0];
            vm.leadPreviewObject.notionObject.SalesforceAccountID = vm.selected.id;
            vm.leadPreviewObject.notionObject.PlayDisplayName = 'Foo';
            vm.leadPreviewObject.notionObject.PlayDescription = 'Bar';
            vm.leadPreviewObject.notionObject.TalkingPoints = talkingPoints;
        });
    };

    function handleLpiPreviewInit(evt) {
        if (evt.data === 'initLpiPreview') {
            window.removeEventListener('message', handleLpiPreviewInit);
            emitLeadObject();
        }
    }

    function emitLeadObject() {
        iframe.contentWindow.postMessage(vm.leadPreviewObject,'*')
    }

    vm.onAccountChange = function() {
        vm.leadPreviewObject.notionObject.SalesforceAccountID = vm.selected.id;
        emitLeadObject();
    };


    $scope.$on('$destroy', function () {
        window.removeEventListener('message', handleLpiPreviewInit);
    });

    if (vm.hasTalkingPoints) {
        vm.init();
    }
});
