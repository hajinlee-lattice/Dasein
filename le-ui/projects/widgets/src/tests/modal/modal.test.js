import template from './modal.test.html';

angular.module('modal.test', ['le.widgets.barchart']).component('modalTest', {
    template: template,

    controller: function () {
        this.$onInit = function () {
            this.initModalWindow();
        };
        this.initModalWindow = function () {
            this.modalSMConfig = {
                'name': "delete_data",
                'type': 'sm',
                'title': 'Warning',
                'titlelength': 100,
                'dischargetext': 'Cancel',
                'dischargeaction': 'cancel',
                'confirmtext': 'Yes, Delete',
                'confirmaction': 'proceed',
                'icon': 'fa fa-exclamation-triangle',
                'iconstyle': {'color': 'white'},
                'confirmcolor': 'blue-button',
                'showclose': true,
                'headerconfig': {'background-color':'#FDC151', 'color':'white'},
                'confirmstyle' : {'background-color':'#FDC151'}
            };
            this.modalCallback = function (args) {
                if (this.modalConfig.dischargeaction === args) {
                    this.toggleModal();
                } else if (this.modalConfig.confirmaction === args) {
                    this.toggleModal();
                    this.submitCleanupJob();

                }
            }


            this.toggleModal = function () {
                var modal = ModalStore.get(this.modalConfig.name);
                if (modal) {
                    modal.toggle();
                }
            }

            this.setBannerMsg = function(showWarning, showSuccess) {
                this.showWarningMsg= showWarning;
                this.showSuccessMsg = showSuccess;
            }

            $scope.$on("$destroy", function () {
                ModalStore.remove(this.modalConfig.name);
            });
        }
    }

});