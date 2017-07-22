angular.module('lp.cg.talkingpoint.preview', [])
.directive('cgTalkingPointPreview', function() {
    return {
        restrict: 'E',
        replace: true,
        scope: {
            play: '='
        },
        templateUrl: 'app/cgtalkingpoint/tppreview/tppreview.component.html',
        controller: 'cgTalkingPointPreviewCtrl',
        controllerAs: 'vm'
    };
})
.controller('cgTalkingPointPreviewCtrl', function ($scope, $stateParams, $sce, $element, CgTalkingPointStore) {
    var iframe = null;

    var vm = this;
    angular.extend(vm, {
        talkingPoints: [],
        leadPreviewObject: null,
        accounts: null,
        selected: null,
        sceIframeSrc: null
    });

    CgTalkingPointStore.getTalkingPoints($stateParams.play_name).then(function(data){
        vm.talkingPoints = data;
        if (vm.talkingPoints.length) {
            vm.init();
        }
    });

    vm.init = function() {
        iframe = $element.find('#tppreview_iframe')[0];
        window.addEventListener('message', handleLpiPreviewInit);

        CgTalkingPointStore.generateLeadPreviewObject({playName: $stateParams.play_name}).then(function(leadPreviewObject){
            vm.leadPreviewObject = leadPreviewObject;
            CgTalkingPointStore.getAccounts().then(function(accounts) {
                vm.accounts = accounts;

                return CgTalkingPointStore.getDanteUrl();
            }).then(function(danteUrl) {
                vm.sceIframeSrc = $sce.trustAsResourceUrl(danteUrl);
            }).then(function() {
                vm.selected = vm.accounts[0];
                vm.leadPreviewObject.notionObject.SalesforceAccountID = vm.selected.id;
                vm.leadPreviewObject.notionObject.PlayDisplayName = $scope.play.display_name;
                vm.leadPreviewObject.notionObject.PlayDescription = $scope.play.description;
                //vm.leadPreviewObject.notionObject.TalkingPoints = vm.talkingPoints;
            });
        });
    };

    function handleLpiPreviewInit(evt) {
        if (evt.data === 'initLpiPreview') {
            window.removeEventListener('message', handleLpiPreviewInit);
            emitLeadObject();
        }
    }

    function emitLeadObject() {
        console.log(vm.leadPreviewObject)
        iframe.contentWindow.postMessage(vm.leadPreviewObject,'*')
    }

    vm.onAccountChange = function() {
        vm.leadPreviewObject.notionObject.SalesforceAccountID = vm.selected.id;
        emitLeadObject();
    };


    $scope.$on('$destroy', function () {
        window.removeEventListener('message', handleLpiPreviewInit);
    });

});
