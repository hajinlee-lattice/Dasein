import './chips-directive.scss';
angular.module('mainApp.appCommon.directives.chips', [])
    .directive('chips', function () {
        return {
            restrict: 'E',
            templateUrl: '/components/chips/chips.component.html',
            scope: {
                icon: '@',
                placeholder: '@',
                datasource: '=',
                callback: '&callbackFunction',
                singleSelection: '=',
                id: '@',
                displayname: '@',
                model: '@',
                queryscope: '@',
                showicon: '@',
                initialvalue: '=',
                initiallyselected: '=',
                allowcustomvalues: '@?',
                enableremoverow: '@?',
                dontrandomizeid: '@?',
                removerowcallback: '&?',
                altlabel: '@',
                context: '=',
                name: '@',
                header: '<',
                nosort: '<',
                sortid: '@',
                sortreverse: '@',
                showloading: '='
            },
            controller: function ($scope) {
                let scope = $scope;
                scope.iconClass = scope.icon;
                scope.showClass = ''
                scope.chips = {};
                scope.positionInQueryList = 0;
                scope.query = '';
                scope.filteredItems = [];
                scope.mouseOut = true;
                scope.blur = true;
                scope.showQueryList = false;
                scope.id = scope.id || scope.displayname;
                scope.showHeader = scope.header != undefined;
                scope.header = scope.header || [];
                scope.nosort = scope.nosort || false;
                scope.sortreverse = scope.sortreverse || false;
                scope.displayName = scope.displayname || scope.id;
                scope.sortId = scope.sortid || scope.id;
                scope.queryScope = scope.queryscope || scope.displayname;
                scope.queryItems = scope.datasource;
                scope.isSelectionDone = false;
                scope.showIcon = scope.showicon;
                scope.customVals = scope.allowcustomvalues ? scope.allowcustomvalues : false;
                scope.removeRow = !scope.enableremoverow ? false : scope.enableremoverow;

                scope.sort = function (sortid) {
                    if (sortid == scope.sortId) {
                        scope.sortreverse = !scope.sortreverse;
                    }

                    scope.sortId = sortid;
                    scope.queryItems = scope.queryItems.sort(scope.sortList);

                    // console.log('[chips] sorting', scope.sortId, !scope.nosort)
                    if (scope.sortreverse) {
                        scope.queryItems.reverse();
                    }
                }

                scope.init = function () {
                    // console.log('[chips] init', scope);
                    if (scope.queryItems && scope.queryItems.length > 0 && !scope.nosort) {
                        scope.sort(scope.sortId);
                    }

                    if (scope.initialvalue != undefined || scope.initialvalue != null) {
                        let initialvalues = [];

                        if (Array.isArray(scope.initialvalue)) {
                            initialvalues = scope.initialvalue;
                        } else {
                            let segmentName = scope.initialvalue.name;
                            initialvalues.push(segmentName);
                        }

                        initialvalues.forEach(value => {
                            if (scope.customVals && typeof value === 'string') {
                                scope.addCustomValue(value, false);
                            } else {
                                let item = scope.datasource.find(data => data[scope.id] == value);
                                scope.chooseItem(item, false);
                            }
                        });
                    }

                    if (scope.initiallyselected) {
                        scope.initiallyselected.forEach((item) => {
                            scope.chooseItem(item, false);
                        });
                    }
                }

                scope.sortList = (objA, objB) => {
                    let a = objA[scope.sortId];
                    let b = objB[scope.sortId];

                    if (typeof a === 'string') {
                        a = a.toLowerCase();
                        b = b.toLowerCase();
                    }

                    if (a > b) {
                        return 1;
                    } else if (a < b) {
                        return -1;
                    } else if (a === b) {
                        return 0;
                    }
                };

                scope.getClassIcon = function () {
                    if (scope.showloading) {
                        return 'fa fa-spinner fa-spin fa-fw';
                    } else if (scope.icon) {
                        return scope.icon;
                    } else {
                        return 'fa fa-search';
                    }
                }

                scope.filterFunction = function (item) {
                    return item[scope.queryScope].toLowerCase().includes(scope.query.toLowerCase()) ? true : false;
                }

                scope.getDisplayName = function (item) {
                    if (item) {
                        return item[scope.displayName];
                    }
                }

                scope.getAltLabel = function (item) {
                    if (item && scope.altlabel) {
                        return item[scope.altlabel];
                    }
                }

                scope.showRemove = () => {
                    if (scope.enableremoverow == 'true') {
                        return true;
                    } else {
                        return false;
                    }
                }

                scope.isSelectionDone = function () {
                    if (scope.singleSelection) {
                        if (Object.keys(scope.chips).length > 0) {
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

                scope.focusInput = (event) => {
                    //if (!scope.query) {
                    scope.blur = false;
                    scope.positionInQueryList = 0;
                    scope.showQueryResult(true);
                    //}
                }

                scope.showQueryResult = function (override) {
                    if (override || scope.query.length > 0) {
                        scope.showQueryList = true;
                    } else {
                        scope.positionInQueryList = 0;
                        scope.showQueryList = false;
                    }

                    return scope.showQueryList;
                }

                scope.queryKeyPressed = function (keyEvent) {
                    scope.blur = false;

                    switch (keyEvent.key) {
                        case 'Backspace':
                        case 'Delete':
                            if (scope.query.length === 0) {
                                scope.mouseOverRow = false;
                                scope.blur = true;
                                scope.clearPositionInQueryList();
                                scope.showQueryList = false;
                            }
                        case 'Escape':
                            scope.positionInQueryList = -1;
                    }

                    if (scope.query.length > 0) {
                        scope.showQueryList = true;
                        switch (keyEvent.key) {
                            case 'ArrowDown':
                                keyEvent.preventDefault()
                                var items = scope.filteredItems;
                                var l = items.length;
                                if (scope.positionInQueryList < scope.filteredItems.length - 1) {
                                    scope.positionInQueryList = scope.positionInQueryList + 1;
                                }
                            case 'ArrowUp':
                                keyEvent.preventDefault()
                                if (scope.positionInQueryList > 0) {
                                    scope.positionInQueryList = scope.positionInQueryList - 1;
                                }
                            case 'Enter':
                                let item = scope.filteredItems[scope.positionInQueryList];
                                if (!item && scope.customVals) {
                                    let obj = scope.getEmptyObject();
                                    obj[scope.displayName] = scope.query;
                                    obj[scope.id] = scope.dontrandomizeid
                                        ? scope.query
                                        : Math.random() + '_' + scope.query;
                                    obj['custom'] = true;
                                    obj.value = scope.query;
                                    scope.addCustomValue(obj);
                                } else {
                                    scope.chooseItem(item);
                                    scope.positionInQueryList = -1;
                                }

                                keyEvent.target.focus();
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
                        scope.callback({ args: Object.values(scope.chips) });
                    }
                }

                scope.setListVisibility = function (visible) {
                    scope.showQueryList = visible;
                }

                scope.chooseItem = function (item, callCallback) {
                    // console.log('chooseItem()', item, scope.query);
                    if (item) {
                        if (scope.singleSelection) {
                            scope.chips = {};
                        }
                        if (scope.chips[item[scope.id]] === undefined) {
                            scope.chips[item[scope.id]] = item;
                        }
                        if (scope.singleSelection) {
                            scope.query = '';
                        }
                        if (callCallback !== false) {
                            scope.callCallback();
                        }
                        if (scope.singleSelection) {
                            scope.setListVisibility(false);
                        }
                    }
                }

                scope.addCustomValue = (value, callCallback) => {
                    // console.log('addCustomValue()', value, scope.query);
                    if (!value) {
                        return false;
                    }

                    scope.query = '';

                    if (typeof value === 'string') {
                        let obj = {};
                        obj[scope.id] = value;
                        scope.chips[value] = obj;
                    } else {
                        scope.chips[value[scope.id]] = value;
                    }

                    if (callCallback !== false) {
                        scope.callCallback();
                    }
                }

                scope.removeItem = function (val) {
                    delete scope.chips[val[scope.id]];
                    scope.callCallback();
                }

                scope.removeRow = (item) => {
                    if (typeof (scope.removerowcallback) != undefined) {
                        scope.removerowcallback({ args: JSON.stringify(item) });
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

                if (scope.context && scope.name) {
                    if (scope.context[scope.name]) {
                        console.warn('Warning: Clobbering "' + scope.name + '" in context controller!');
                    }
                    scope.context[scope.name] = scope;
                }

                scope.init();
            }
        }
    });