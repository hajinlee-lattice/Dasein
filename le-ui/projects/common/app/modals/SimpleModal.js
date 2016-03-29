angular.module('mainApp.appCommon.modals.SimpleModal', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.service('SimpleModal', function (ResourceUtility) {
    
    this.show = function (overrides, okCallback) {
        
        var options = {
            showCloseButton: true,
            height: 250,
            width: 500,
            title: "",
            message: "",
            okButtonLabel: ResourceUtility.getString("BUTTON_OK_LABEL")
        };
        
        angular.extend(options, overrides);
        var modalClass = "no-close-button";
        var modalElement = $('<div></div>');
        modalElement.html(options.message);
        modalElement.dialog({
            height: options.height,
            width: options.width,
            autoOpen: false,
            closeOnEscape: false,
            modal: true,
            title: options.title,
            dialogClass: options.showCloseButton === true ? '' : modalClass,
            buttons: [
                {
                    text: options.okButtonLabel,
                    click: function () {
                        if (okCallback != null && typeof okCallback === 'function') {
                            okCallback();
                        }
                        modalElement.dialog("close");
                    }
                }
            ],
            close: function (evt, ui) {
                modalElement.remove();
            }
       });
       
       modalElement.dialog("open" );
    };
});