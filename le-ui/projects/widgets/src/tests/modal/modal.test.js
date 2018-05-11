import template from './modal.test.html';
import "../../components/modal/modal.component";

angular.module('modal.test', ['le.widgets.modal']).
component('modalTest', {
    template: template,

    controller: ['ModalStore', function (ModalStore) {
        this.$onInit = function () {
            this.initModalWindow();
            thid.configSm = {
                'name': "delete_data",
                'type': 'sm',
                'title': 'Warning',
                'titlelength': 100,
                'dischargetext': 'Cancel',
                'dischargeaction': 'cancel',
                'confirmtext': 'Yes, Delete',
                'confirmaction': 'proceed',
                'icon': 'fa fa-exclamation-triangle',
                'iconstyle': {
                    'color': 'white'
                },
                'confirmcolor': 'blue-button',
                'showclose': true,
                'headerconfig': {
                    'background-color': '#FDC151',
                    'color': 'white'
                },
                'confirmstyle': {
                    'background-color': '#FDC151'
                }
            };
            this.configMedium = {
                'name': "delete_data",
                'type': 'md',
                'title': 'Warning',
                'titlelength': 100,
                'dischargetext': 'Cancel',
                'dischargeaction': 'cancel',
                'confirmtext': 'Yes, Delete',
                'confirmaction': 'proceed',
                'icon': 'fa fa-exclamation-triangle',
                'iconstyle': {
                    'color': 'white'
                },
                'confirmcolor': 'blue-button',
                'showclose': true,
                'headerconfig': {
                    'background-color': '#FDC151',
                    'color': 'white'
                },
                'confirmstyle': {
                    'background-color': '#FDC151'
                }
            };
            this.configLarge = {
                'name': "delete_data",
                'type': 'lg',
                'title': 'Warning',
                'titlelength': 100,
                'dischargetext': 'Cancel',
                'dischargeaction': 'cancel',
                'confirmtext': 'Yes, Delete',
                'confirmaction': 'proceed',
                'icon': 'fa fa-exclamation-triangle',
                'iconstyle': {
                    'color': 'white'
                },
                'confirmcolor': 'blue-button',
                'showclose': true,
                'headerconfig': {
                    'background-color': '#FDC151',
                    'color': 'white'
                },
                'confirmstyle': {
                    'background-color': '#FDC151'
                }
            };
        };
        this.initSmModalWindow = function () {
            this.smModalCallback = function (args) {
                if (this.config.dischargeaction === args) {
                    this.toggleModal();
                } else if (this.config.confirmaction === args) {
                    this.toggleModal();
                    this.submitCleanupJob();

                }
            }


            this.toggleModal = function () {
                var modal = ModalStore.get(this.config.name);
                if (modal) {
                    modal.toggle();
                }
            }

            this.setBannerMsg = function (showWarning, showSuccess) {
                this.showWarningMsg = showWarning;
                this.showSuccessMsg = showSuccess;
            }

            this.openModal = function () {
                this.toggleModal();
            }

            this.$onDestroy = function () {
                console.log('Destroying the modal');
                ModalStore.remove(this.config.name);
            };
        }
    }]

});