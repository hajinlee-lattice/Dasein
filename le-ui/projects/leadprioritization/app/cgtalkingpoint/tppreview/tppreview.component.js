angular.module('lp.cg.talkingpoint.preview', [])
.controller('cgTalkingPointPreviewCtrl', function ($scope, $sce, $element, CgTalkingPointStore) {

    var iframe = $element.find('#tppreview_iframe')[0];

    $scope.leadPreviewObject = CgTalkingPointStore.generateLeadPreviewObject();
    CgTalkingPointStore.getAccounts().then(function(accounts) {
        $scope.accounts = accounts;

        return CgTalkingPointStore.getDanteUrl();
    }).then(function(danteUrl) {
        $scope.sceIframeSrc = $sce.trustAsResourceUrl(danteUrl);
    }).then(function() {
        $scope.selected = $scope.accounts[0];
        $scope.playName = 'Foo play';
        $scope.playDescription = 'This is bar play';
        $scope.leadPreviewObject.notionObject.SalesforceAccountID = $scope.selected.id;
        $scope.leadPreviewObject.notionObject.PlayDisplayName = $scope.playName;
        $scope.leadPreviewObject.notionObject.PlayDescription = $scope.playDescription;
        $scope.leadPreviewObject.notionObject.TalkingPoints = CgTalkingPointStore.getTalkingPoints();
        $scope.hasTalkingPoints = $scope.leadPreviewObject.notionObject.TalkingPoints.length > 0;
    });

    window.addEventListener('message', handleLpiPreviewInit);

    function handleLpiPreviewInit(evt) {
        if (evt.data === 'initLpiPreview') {
            emitLeadObject();
            window.removeEventListener('message', handleLpiPreviewInit);
        }
    }

    $scope.onAccountChange = function() {
        $scope.leadPreviewObject.notionObject.SalesforceAccountID = $scope.selected.id;
        emitLeadObject();
    };

    function emitLeadObject() {
        iframe.contentWindow.postMessage($scope.leadPreviewObject,'*')
    }

    $scope.$on('$destroy', function () {
        window.removeEventListener('message', handleLpiPreviewInit);
    });
});
