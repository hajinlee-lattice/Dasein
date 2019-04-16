import './chips-directive.scss';
angular.module('mainApp.appCommon.directives.chips', [])
.directive('chips', function () {
    return {
        restrict: 'E',
        templateUrl: '/app/directives/chips.component.html',
        scope: { 
            icon: '@',
            placeholder: '@', 
            datasource: '=', 
            callback: '&callbackFunction', 
            singleSelection: '=', 
            id: '@', 
            displayname: '@' ,
            model: '@',
            queryscope: '@',
            showicon: '@',
            initialvalue: '=',
            initiallyselected: '=',
            allowcustomvalues:'@?',
            enableremoverow: '@?',
            removerowcallback:'&?'
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
            scope.customVals = scope.allowcustomvalues ? scope.allowcustomvalues : false
            scope.removeRow = !scope.enableremoverow ? false : scope.enableremoverow
            scope.sortList = (objA, objB) => {
                let a = objA[scope.displayname];
                let b = objB[scope.displayname];
                a = a.toLowerCase();
                b = b.toLowerCase();
              
                if (a > b) {
                  return 1;
                } else if (a < b) {
                  return -1;
                } else if (a === b) {
                  return 0;
                }
            };

            scope.getClassIcon = function(){
                console.log('SEARCH ', scope.icon);
                if(scope.icon){
                    return scope.icon;
                }else{
                    return 'fa fa-search';
                }
            }
            scope.filterFunction = function(item) {
                return item[scope.queryScope].toLowerCase().includes( scope.query.toLowerCase() ) ? true : false;
            };

            scope.getDisplayName = function(item){
                if(item){
                    return item[scope.displayName];
                }
            }
            scope.showRemove = () => {
                if(scope.enableremoverow == 'true'){
                    return true;
                }else {
                    return false;
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
                if('Escape' === keyEvent.key){
                    scope.positionInQueryList = -1;
                }
                if (keyEvent.key === 'Enter' && scope.query.length > 0) {
                        let item = scope.filteredItems[scope.positionInQueryList];
                        if(!item && scope.customVals){
                            let obj = scope.getEmptyObject();
                            obj[scope.displayName] = scope.query;
                            obj[scope.id] = Math.random()+'_'+scope.query;
                            obj['custom'] = true;
                            obj.value = scope.query;
                            scope.addCustomValue(obj);
                        }else{
                            scope.chooseItem(item);
                            scope.positionInQueryList = -1;
                        }
                }
            }

            scope.getEmptyObject = () => {
                let obj = {};
                obj[scope.displayName] = undefined;
                obj[scope.id] = undefined;
                obj['value'] = undefined;
                return obj;
            }

            scope.callCallback = function () {
                if (typeof (scope.callback) != undefined) {
                    scope.callback({args:Object.values(scope.chips)});
                }
            }

            scope.setListVisibility = function (visible) {
                scope.showQueryList = visible;
            }

            scope.chooseItem = function (item, callCallback) {
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
                    if(callCallback !== false){
                        scope.callCallback();
                    }
                    if (scope.singleSelection){
                        scope.setListVisibility(false);
                    }
                }

                // console.log(scope.chips);
            }
            scope.addCustomValue = (value) => {
                scope.query = '';
                scope.chips[value[scope.id]] = value;
                scope.callCallback();
            }
            scope.removeItem = function (val) {
                delete scope.chips[val[scope.id]];
                scope.callCallback();
            }

            scope.removeRow = (item) => {
                if (typeof (scope.removerowcallback) != undefined) {
                    scope.removerowcallback({args:JSON.stringify(item)});
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
            if(scope.queryItems && scope.queryItems.length > 0){
                scope.queryItems = scope.queryItems.sort(scope.sortList);
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
            if(scope.initiallyselected){
                scope.initiallyselected.forEach((item) => {
                    scope.chooseItem(item, false);
                });
            }

        }
    }
});