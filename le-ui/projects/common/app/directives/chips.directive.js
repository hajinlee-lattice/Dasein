angular.module('mainApp.appCommon.directives.chips', [])
.directive('chips', function () {
    return {
        restrict: 'E',
        templateUrl: '/components/ai/chips.component.html',
        scope: { placeholder: '@', datasource: '=', callback: '&callbackFunction', singleSelection: '=' },
        link: function (scope, element, attrs, ctrl) {

            scope.showClass = ''
            scope.chips = {};
            scope.positionInQueryList = 0;
            scope.query = '';
            scope.filteredItems = [];
            scope.queryItems = scope.datasource;
            scope.mouseOut = true;
            scope.blur = true;
            scope.showQueryList = false;

            scope.isSelectionDone = function(){
                if(Object.keys(scope.chips).length > 0 && scope.singleSelection){
                    return true;
                } else {
                    return false;
                }
            }

            scope.isRowSelected = function (index) {

                if (index == scope.positionInQueryList) {
                    return true;
                } else {
                    return false;
                }
            }
            scope.blurText = function () {
                if (scope.mouseOut && scope.blur) {
                    scope.showQueryList = false;
                }
                scope.blur = true;
            }

            scope.showQueryResult = function () {

                if (scope.query.length > 0) {
                    scope.showQueryList = true;
                } else {
                    scope.positionInQueryList = 0;
                    scope.showQueryList = false;
                }
                return scope.showQueryList;
            }
            scope.queryKeyPressed = function (keyEvent) {
                scope.blur = false;
                if (scope.query.length >= 0) {
                    scope.showQueryList = true;
                }

                if ('ArrowDown' === keyEvent.key) {
                    keyEvent.preventDefault()
                    var items = scope.filteredItems;
                    var l = items.length;
                    if (scope.positionInQueryList < scope.filteredItems.length - 1) {
                        scope.positionInQueryList = scope.positionInQueryList + 1;
                    }
                }
                if ('ArrowUp' === keyEvent.key) {
                    keyEvent.preventDefault()
                    if (scope.positionInQueryList > 0) {
                        scope.positionInQueryList = scope.positionInQueryList - 1;
                    }
                }
                if (keyEvent.key === 'Enter') {
                    scope.chooseItem(scope.filteredItems[scope.positionInQueryList]);
                    scope.positionInQueryList = -1;

                }
            }
            scope.chooseItem = function (item) {
                if (item) {
                    if (scope.chips[item.id] === undefined) {
                        scope.chips[item.id] = item;
                    }
                    if(scope.singleSelection){
                        scope.query = '';   
                    }
                    scope.callCallback();
                }
            }
            scope.removeItem = function (id) {
                delete scope.chips[id];
                scope.callCallback();
            }

            scope.callCallback = function () {
                if (typeof (scope.callback) != undefined) {
                    scope.callback({args:scope.chips});
                }
            }

            scope.hoverIn = function () {
                scope.showQueryList = true;
                scope.mouseOverRow = true;
                scope.clearPositionInQueryList();
            }
            scope.hoverOut = function () {
                scope.mouseOverRow = false;
                if (scope.blur) {
                    scope.clearPositionInQueryList();
                    scope.showQueryList = false;
                }
            }

            scope.clearPositionInQueryList = function () {
                scope.positionInQueryList = -1;
            }
            scope.clearQuery = function () {
                scope.query = '';
            }

            scope.setListVisibility = function (visible) {
                scope.showQueryList = visible;
            }
        }
    }
});