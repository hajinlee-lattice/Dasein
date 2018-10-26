angular.module('mainApp.appCommon.directives.chips', [])
.directive('chips', function () {
    return {
        restrict: 'E',
        templateUrl: '/components/ai/chips.component.html',
        scope: { 
            placeholder: '@', 
            datasource: '=', 
            callback: '&callbackFunction', 
            singleSelection: '=', 
            id: '@', 
            displayname: '@' ,
            model: '@',
            queryscope: '@',
            showicon: '@',
            initialvalue: '='
        },
        link: function (scope, element, attrs, ctrl) {
            scope.showClass = ''
            scope.chips = {};
            scope.positionInQueryList = 0;
            scope.query = '';
            scope.filteredItems = [];
            scope.mouseOut = true;
            scope.blur = true;
            scope.showQueryList = false;
            scope.displayName = scope.displayname;
            scope.queryItems = scope.datasource;
            scope.queryScope = scope.queryscope;
            scope.isSelectionDone = false;
            scope.showIcon = scope.showicon;

            // console.log(scope);

            scope.filterFunction = function(item) {
                return item[scope.queryScope].toLowerCase().includes( scope.query.toLowerCase() ) ? true : false;
            };

            scope.getDisplayName = function(item){
                if(item){
                    return item[scope.displayName];
                }
            }

            scope.isSelectionDone = function(){
                if(scope.singleSelection){
                    if(Object.keys(scope.chips).length > 0){
                        return true;
                    } else {
                        return false;
                    }
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
                if (scope.query.length > 0) {
                    scope.showQueryList = true;
                }
                if(('Backspace' === keyEvent.key || 'Delete' === keyEvent.key) && scope.query.length === 0){
                    scope.mouseOverRow = false;
                    scope.blur = true;
                    scope.clearPositionInQueryList();
                    scope.showQueryList = false;
                }
                if ('ArrowDown' === keyEvent.key && scope.query.length > 0) {
                    keyEvent.preventDefault()
                    var items = scope.filteredItems;
                    var l = items.length;
                    if (scope.positionInQueryList < scope.filteredItems.length - 1) {
                        scope.positionInQueryList = scope.positionInQueryList + 1;
                    }
                }
                if ('ArrowUp' === keyEvent.key && scope.query.length > 0) {
                    keyEvent.preventDefault()
                    if (scope.positionInQueryList > 0) {
                        scope.positionInQueryList = scope.positionInQueryList - 1;
                    }
                }
                if (keyEvent.key === 'Enter' && scope.query.length > 0) {
                    scope.chooseItem(scope.filteredItems[scope.positionInQueryList]);
                    scope.positionInQueryList = -1;

                }
            }

            scope.callCallback = function () {
                if (typeof (scope.callback) != undefined) {
                    scope.callback({args:Object.values(scope.chips)});
                }
            }

            scope.setListVisibility = function (visible) {
                scope.showQueryList = visible;
            }

            scope.chooseItem = function (item) {
                if (item) {
                    if(scope.singleSelection) {
                        scope.chips = {};
                    }
                    if (scope.chips[item[scope.id]] === undefined) {
                        scope.chips[item[scope.id]] = item;
                    }
                    if(scope.singleSelection){
                        scope.query = '';
                    }
                    
                    scope.callCallback();
                    if (scope.singleSelection){
                        scope.setListVisibility(false);
                    }
                }

                // console.log(scope.chips);
            }
            scope.removeItem = function (val) {
                delete scope.chips[val[scope.id]];
                scope.callCallback();
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

            if (scope.initialvalue != undefined || scope.initialvalue != null) {

                let initialvalues = [];
                if(Array.isArray(scope.initialvalue)){
                    initialvalues = scope.initialvalue;
                } else {
                    let segmentName = scope.initialvalue.name;
                    initialvalues.push(segmentName);
                }

                let array1 = scope.datasource;
                let array2 = initialvalues;
                let array = array2.map(function(e) {
                    let f = array1.find(a => a[scope.id] == e);
                    scope.chooseItem(f);
                });
            }

        }
    }
});