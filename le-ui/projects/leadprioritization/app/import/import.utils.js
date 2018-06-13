/**
 * Account:[
 *  {name: 'the_name', fieldType: 'TEXT', requiredIfNoField: boolean/null, requiredType:'Required'/'NotRequired'}
 * ]
 */
angular.module('lp.import.utils', [])
.service('ImportUtils', function(){
    var ImportUtils = this;
    ImportUtils.ACCOUNT_ENTITY = 'Account';
    var latticeSchema = {};

    function setFields(fieldObj, field){
        Object.keys(field).forEach(function(key){
            fieldObj[key] = field[key];
        });
    }

    function setFieldsInEntity(entityName, fields){
        var entity = latticeSchema.entityName;
        if(!entity) {
            entity = { map:{}, list:[]};
            latticeSchema[entityName] = entity;
        }
        var keys = Object.keys(fields);
        keys.forEach(function(field) {
            var fieldName = fields[field].name;
            var fieldObj = {};
            entity.map[fieldName] = fieldObj;
            setFields(fieldObj, fields[field]);
            entity.list.push(fieldObj);
        });
    }

    function isFieldMapped(entity, fieldName, fieldsMapped){
        var mapped = false;
        
        if(fieldsMapped){
            var entityObj = latticeSchema[entity];
            var map = entityObj.map;
            fieldsMapped.forEach(function(element){
                if(element.userField == fieldName && element.mappedField != null && map[element.mappedField]){
                    mapped = true;
                    return mapped;
                }
            });
        }
       
        return mapped;
    }

    function cleanSchema(){
        latticeSchema = {};
    }
    /**
     * It sets the entities and all the fields associated with it
     */
    this.setLatticeSchema = function(apiResult){
        cleanSchema();
        var entities = Object.keys(apiResult);
        entities.forEach(function(entity){
            setFieldsInEntity(entity, apiResult[entity]);
        });
        // console.log('Schema ', latticeSchema);
    };


    this.isFieldInSchema = function(entity, fieldName, fieldsMapped){
        var inschema = false;
        var entityObj = latticeSchema[entity];
        if(entityObj && entityObj.map[fieldName]){
            inschema = true;
        }else{
            inschema = isFieldMapped(entity, fieldName, fieldsMapped);
        }
        return inschema;
    };

    function setMapping(savedObj, fieldsMapped){
        var keysMapped = Object.keys(fieldsMapped);
        keysMapped.forEach(function(mapped){
            if(savedObj.mappedField === fieldsMapped[mapped].mappedField && 
                savedObj.userField !== fieldsMapped[mapped].userField){
                fieldsMapped[mapped].mappedField =  null;
                fieldsMapped[mapped].mappedToLatticeField = false;
            }
            if(savedObj.userField === fieldsMapped[mapped].userField){
                fieldsMapped[mapped].mappedField = savedObj.mappedField;
            }
        });
    }
   
    this.updateDocumentMapping = function(savedObj, fieldsMapping){
        if(savedObj && fieldsMapping){
            var keysSaved = Object.keys(savedObj);
            
            keysSaved.forEach(function(keySaved){
                setMapping(savedObj[keySaved], fieldsMapping);
            });
        }
    };


});