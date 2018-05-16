/** 
 * Component allows to edit two fields, name (text) and description (textarea).
 * It also has two buttons to trigger the saving or cancel it.
 * It takes care of the validation of the mandatory fields.
 * Name is the mandatory field.
 * 
 * dataobj: is te object containing the original data
 * callback: method to call once save/cancel button is clicked
 * saving: vaalue that indicate if the saving is in progress
 * config : {
 *      data: {id: 'id'},
 *      fields:{
 *          name: {fieldname: 'displayName', visible: true, maxLength: 50, label:'Name'},
 *          description: {fieldname: 'description', visible: false, maxLength: 1000, label: 'Description'}
 *      }  
 * }
 */
angular.module('lp.tile.edit', [])
    .component('editForm', {
        templateUrl: '/components/forms/edit-form.component.html',
        bindings: {
            config: '<',
            dataobj:'=',
            callback: '&',
            saving: '='
        },
        controller: function () {
            
            this.showName = this.config.fields.name.visible;
            this.showDescription = this.config.fields.description.visible;
            this.name = this.dataobj[this.config.fields.name.fieldname];
            this.description = this.dataobj[this.config.fields.description.fieldname];

            this.cancel = function($event){
                $event.stopPropagation();
                this.callback({
                    obj: this.dataobj
                });
            };

            this.isValid = function(editNameDescription){
                return editNameDescription.$valid;
            };

            this.isModified = function(editNameDescription){
                return editNameDescription.$dirty;
            };

            this.save = function($event){
                $event.stopPropagation();
                var idKey = this.config.data.id;
                var nameKey = this.config.fields.name.fieldname;
                var descriptionKey = this.config.fields.description.fieldname;
                var newData = {};

                newData[idKey] = this.dataobj[idKey];
                newData[nameKey] = this.name;
                newData[descriptionKey] = this.description;
                this.callback({
                    obj: this.dataobj,
                    newData: newData
                });
            };
        }

    });