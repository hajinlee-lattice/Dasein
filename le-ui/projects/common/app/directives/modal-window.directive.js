angular.module('mainApp.appCommon.directives.modal.window', [])
.service('ModalStore', function () {
    this.modals = {};




    this.get = function (name) {
        return this.modals[name].modal;
    }

    this.getData = function(name){
        var tmp = this.modals[name].data;
        if(tmp && tmp !== undefined){
            return this.modals[name].data ;
        }else{
            return {};
        }
    }
    this.setData = function(name, data){
        var modal = this.modals[name];
        if(modal){
            this.modals[name].data = data;
        }
    }

    this.set = function (name, modal) {
        var mod = this.modals[name];
        if(!mod){
            var modalObj = {
                modal: modal
            };
            this.modals[name] = modalObj;
        }
        // this.modals[name] = modal;
    }
    this.remove = function(name) {
        if(this.modals[name]){
            delete this.modals[name];
        }
    }
})
.filter('cut', function () {
    return function (value, wordwise, max, tail) {
        if (!value) {
            return '';
        }
        max = parseInt(max, 10);
        if (!max) return value;
        if (value.length <= max) return value;

        value = value.substr(0, max);
        if (wordwise) {
            var lastspace = value.lastIndexOf(' ');
            if (lastspace !== -1) {
                if (value.charAt(lastspace - 1) === '.' || value.charAt(lastspace - 1) === ',') {
                    lastspace = lastspace - 1;
                }
                value = value.substr(0, lastspace);
            }
        }

        return value + (tail || ' …');

    }
})
.directive('leModalWindow', ['ModalStore', function (modalStore) {
    return {
        restrict: 'E',
        scope: { config: '=', callback: '&callbackFunction' },
        replace: true,
        transclude: true,
        templateUrl: "/components/modal/modal-window.component.html",
        link: function (scope, element, attrs, ctrl, transclude, ModalStore) {

            scope.opened = false;
            scope.showModalMsg = false;
            scope.showWaiting = false;
            scope.dischargeDisabled = false;
            scope.confirmDisabled = false;
            scope.modalMsg = '';
            scope.modalMsgType = 'warning';
            scope.iconSupported = {
                "warning": "fa fa-exclamation-triangle"
            };
            if (!scope.config) {
                scope.config = {};
            }
            var name = scope.config['name'] || Date.now();

            modalStore.set(name, scope);
            // console.log('Color', scope.config.headerconfig )
            scope.modalConfig = {
                "type": scope.config.type || "md",
                "icon": scope.config.icon || scope.iconSupported.warning,
                "iconstyle" : scope.config.iconstyle || {'background-color':'black'},
                "title": scope.config.title || "Default Title",
                "titlelength": scope.config.titlelength || 100,
                "showclose": typeof scope.config.showclose === 'undefined' ? true : scope.config.showclose,
                "dischargetext": scope.config.dischargetext || "Cancel",
                "dischargeaction" : scope.config.dischargeaction || 'cancel',
                "confirmtext": scope.config.confirmtext || "OK",
                "confirmaction" : scope.config.confirmaction || 'ok',
                "contenturl": scope.config.contenturl || '',
                "headerconfig": scope.config.headerconfig || {},
                "confirmstyle" : scope.config.confirmstyle || {},
                "confirmcolor": scope.config.confirmstyle === undefined ? (scope.config.confirmcolor || 'blue-button') : '',
            };

            scope.toggle = function (data) {
                scope.opened = !scope.opened;
                modalStore.setData(name, data);
                resetWindow();
            }

            scope.showMessage = function (msg, type) {
                scope.showModalMsg = true;
                scope.modalMsg = msg;
                scope.modalMsgType = type;
            }
            scope.waiting = function(show){
                scope.showWaiting = show;
                if(show === true){
                    scope.disableConfirmButton(true);
                }
            }

            scope.disableConfirmButton = function (disable) {
                this.confirmDisabled = disable;
            }

            scope.disableDischargeButton = function (disable) {
                scope.dischargeDisabled = disable;
            }

            scope.forceDischarge = function(){
                scope.toggle();
                resetWindow();
                scope.callCallback('closedForced');

            }

            scope.discharge = function () {
                scope.callCallback(scope.modalConfig.dischargeaction);
            }
            scope.confirm = function () {
                scope.callCallback(scope.modalConfig.confirmaction);
            }
            scope.callCallback = function (value) {
                if (typeof (scope.callback) != undefined) {
                    var data = modalStore.getData(name);
                    scope.callback({ args: {action: value, data: data }});
                }
            }

            function resetWindow() {
                scope.showModalMsg = false;
                scope.dischargeDisabled = false;
                scope.confirmDisabled = false;
                scope.showWaiting = false;
                scope.modalMsg = '';
            }

        }
    };
}]);

