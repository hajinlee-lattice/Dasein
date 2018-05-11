import template from './modal.component.html';

angular.module('le.widgets.modal', [])
    .service('ModalStore', function () {
        this.modals = {};

        this.get = function (name) {
            return this.modals[name];
        }

        this.set = function (name, modal) {
            this.modals[name] = modal;
        }
        this.remove = function (name) {
            if (this.modals[name]) {
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
    .component('leModal', {
        

        bindings: {
            config: '=',
            callback: '&callbackFunction'
        },
        template: template,
        transclude: true,

        controller: ['ModalStore', function (ModalStore) {
            var self = this;
            this.$onInit = function () {


            };

            this.opened = false;
            this.showModalMsg = false;
            this.showWaiting = false;
            this.dischargeDisabled = false;
            this.confirmDisabled = false;
            this.modalMsg = '';
            this.modalMsgType = 'warning';
            this.iconSupported = {
                "warning": "fa fa-exclamation-triangle"
            };
            if (!this.config) {
                this.config = {};
            }
            var date = Date.now();
            var name = this.config['name'] || date;

            ModalStore.set(name, this);
            // console.log('Color', this.config.headerconfig )
            this.modalConfig = {
                "type": this.config.type || "md",
                "icon": this.config.icon || this.iconSupported.warning,
                "iconstyle": this.config.iconstyle || {
                    'background-color': 'black'
                },
                "title": this.config.title || "Default Title",
                "titlelength": this.config.titlelength || 100,
                "showclose": typeof this.config.showclose === 'undefined' ? true : this.config.showclose,
                "dischargetext": this.config.dischargetext || "Cancel",
                "dischargeaction": this.config.dischargeaction || 'cancel',
                "confirmtext": this.config.confirmtext || "OK",
                "confirmaction": this.config.confirmaction || 'ok',
                "contenturl": this.config.contenturl || '',
                "headerconfig": this.config.headerconfig || {},
                "confirmstyle": this.config.confirmstyle || {},
                "confirmcolor": this.config.confirmstyle === undefined ? (this.config.confirmcolor || 'blue-button') : '',
            };

            this.toggle = function () {
                this.opened = !this.opened;
                resetWindow();
            }

            this.showMessage = function (msg, type) {
                this.showModalMsg = true;
                this.modalMsg = msg;
                this.modalMsgType = type;
            }
            this.waiting = function (show) {
                this.showWaiting = show;
                if (show === true) {
                    this.disableConfirmButton(true);
                }
            }

            this.disableConfirmButton = function (disable) {
                this.confirmDisabled = disable;
            }

            this.disableDischargeButton = function (disable) {
                this.dischargeDisabled = disable;
            }

            this.forceDischarge = function () {
                this.toggle();
                resetWindow();
                this.callCallback('closedForced');

            }

            this.discharge = function () {
                this.callCallback(this.modalConfig.dischargeaction);
            }
            this.confirm = function () {
                this.callCallback(this.modalConfig.confirmaction);
            }
            this.callCallback = function (value) {
                if (typeof (this.callback) != undefined) {
                    this.callback({
                        args: value
                    });
                }
            }

            function resetWindow() {
                self.showModalMsg = false;
                self.dischargeDisabled = false;
                self.confirmDisabled = false;
                self.showWaiting = false;
                self.modalMsg = '';
            }
        }]

    });


// angular.module('mainApp.appCommon.directives.modal.window', [])
//     .service('ModalStore', function () {
//         this.modals = {};

//         this.get = function (name) {
//             return this.modals[name];
//         }

//         this.set = function (name, modal) {
//             this.modals[name] = modal;
//         }
//         this.remove = function (name) {
//             if (this.modals[name]) {
//                 delete this.modals[name];
//             }
//         }
//     })
//     .filter('cut', function () {
//         return function (value, wordwise, max, tail) {
//             if (!value) {
//                 return '';
//             }
//             max = parseInt(max, 10);
//             if (!max) return value;
//             if (value.length <= max) return value;

//             value = value.substr(0, max);
//             if (wordwise) {
//                 var lastspace = value.lastIndexOf(' ');
//                 if (lastspace !== -1) {
//                     if (value.charAt(lastspace - 1) === '.' || value.charAt(lastspace - 1) === ',') {
//                         lastspace = lastspace - 1;
//                     }
//                     value = value.substr(0, lastspace);
//                 }
//             }

//             return value + (tail || ' …');

//         }
//     })
//     .directive('leModalWindow', ['ModalStore', function (modalStore) {
//         return {
//             restrict: 'E',
//             scope: {
//                 config: '=',
//                 callback: '&callbackFunction'
//             },
//             replace: true,
//             transclude: true,
//             templateUrl: "/components/modal/modal-window.component.html",
//             link: function (scope, element, attrs, ctrl, transclude, ModalStore) {

//                 scope.opened = false;
//                 scope.showModalMsg = false;
//                 scope.showWaiting = false;
//                 scope.dischargeDisabled = false;
//                 scope.confirmDisabled = false;
//                 scope.modalMsg = '';
//                 scope.modalMsgType = 'warning';
//                 scope.iconSupported = {
//                     "warning": "fa fa-exclamation-triangle"
//                 };
//                 if (!scope.config) {
//                     scope.config = {};
//                 }
//                 var name = scope.config['name'] || Date().now();

//                 modalStore.set(name, scope);
//                 // console.log('Color', scope.config.headerconfig )
//                 scope.modalConfig = {
//                     "type": scope.config.type || "md",
//                     "icon": scope.config.icon || scope.iconSupported.warning,
//                     "iconstyle": scope.config.iconstyle || {
//                         'background-color': 'black'
//                     },
//                     "title": scope.config.title || "Default Title",
//                     "titlelength": scope.config.titlelength || 100,
//                     "showclose": typeof scope.config.showclose === 'undefined' ? true : scope.config.showclose,
//                     "dischargetext": scope.config.dischargetext || "Cancel",
//                     "dischargeaction": scope.config.dischargeaction || 'cancel',
//                     "confirmtext": scope.config.confirmtext || "OK",
//                     "confirmaction": scope.config.confirmaction || 'ok',
//                     "contenturl": scope.config.contenturl || '',
//                     "headerconfig": scope.config.headerconfig || {},
//                     "confirmstyle": scope.config.confirmstyle || {},
//                     "confirmcolor": scope.config.confirmstyle === undefined ? (scope.config.confirmcolor || 'blue-button') : '',
//                 };

//                 scope.toggle = function () {
//                     scope.opened = !scope.opened;
//                     resetWindow();
//                 }

//                 scope.showMessage = function (msg, type) {
//                     scope.showModalMsg = true;
//                     scope.modalMsg = msg;
//                     scope.modalMsgType = type;
//                 }
//                 scope.waiting = function (show) {
//                     scope.showWaiting = show;
//                     if (show === true) {
//                         scope.disableConfirmButton(true);
//                     }
//                 }

//                 scope.disableConfirmButton = function (disable) {
//                     this.confirmDisabled = disable;
//                 }

//                 scope.disableDischargeButton = function (disable) {
//                     scope.dischargeDisabled = disable;
//                 }

//                 scope.forceDischarge = function () {
//                     scope.toggle();
//                     resetWindow();
//                     scope.callCallback('closedForced');

//                 }

//                 scope.discharge = function () {
//                     scope.callCallback(scope.modalConfig.dischargeaction);
//                 }
//                 scope.confirm = function () {
//                     scope.callCallback(scope.modalConfig.confirmaction);
//                 }
//                 scope.callCallback = function (value) {
//                     if (typeof (scope.callback) != undefined) {
//                         scope.callback({
//                             args: value
//                         });
//                     }
//                 }

//                 function resetWindow() {
//                     scope.showModalMsg = false;
//                     scope.dischargeDisabled = false;
//                     scope.confirmDisabled = false;
//                     scope.showWaiting = false;
//                     scope.modalMsg = '';
//                 }

//             }
//         };
//     }]);