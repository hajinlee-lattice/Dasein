/**
 * Account:[
 *  {name: 'the_name', fieldType: 'TEXT', requiredIfNoField: boolean/null, requiredType:'Required'/'NotRequired'}
 * ]
 */
angular.module('lp.import.utils', ['mainApp.core.redux'])
.service('ImportUtils', function(ReduxService){
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
    
    function isFieldPartOfSchema(entity, fieldName){
        var entityObj = latticeSchema[entity];
        var map = entityObj.map;
        if(map[fieldName]){
            return true;
        }else{
            return false;
        }
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
    
    this.getOriginalMapping = (entity, fieldMappingsApi) => {
        // clearInitialMapping();
        let originalMapping = {fieldMappings: {entity: '', list: [], map: {}}};
        let keys = Object.keys(fieldMappingsApi);
        originalMapping.fieldMappings.entity = entity;
        keys.forEach((key) => {
            let userField = fieldMappingsApi[key].userField;
            originalMapping.fieldMappings.list.push(fieldMappingsApi[key]);
            originalMapping.fieldMappings.map[userField] = fieldMappingsApi[key];
        });
        // console.log('ORIGINAL MAPPING ==> ',originalMapping);
        return originalMapping;
    }

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

    this.remapTypes = function(fieldsMapped, newTypesObj){
        var userFields = Object.keys(newTypesObj);
        userFields.forEach(function(userFieldName){
            updateUserFieldType(fieldsMapped,userFieldName, newTypesObj[userFieldName]);
        });
    };

    function updateUserFieldType(fieldsMapped, userFieldName, newTypeObj){
        fieldsMapped.forEach(function(fieldMapped){
            if(fieldMapped.userField == userFieldName){
                fieldMapped.fieldType = newTypeObj.type;
                updateFieldDate(fieldMapped, newTypeObj);
                return;
            }
        });
    }

    function updateFieldDate(fieldMapped, newTypeObj) {
        if(fieldMapped.fieldType !== 'DATE'){
            fieldMapped.dateFormatString = null;
            fieldMapped.timeFormatString = null
            fieldMapped.timezone = null;
        }else{
            fieldMapped.dateFormatString = newTypeObj.dateFormatString;
            fieldMapped.timeFormatString = newTypeObj.timeFormatString;
            fieldMapped.timezone = newTypeObj.timezone;
        }
    }

    function setMapping(entity, savedObj, fieldsMapped){
        originalMapping = fieldsMapping ? fieldsMapping : {};
        var keysMapped = Object.keys(fieldsMapped);
        keysMapped.forEach(function(mapped){
            if(savedObj.mappedField === fieldsMapped[mapped].mappedField && 
                savedObj.userField !== fieldsMapped[mapped].userField){
                fieldsMapped[mapped].mappedField =  null;
                fieldsMapped[mapped].mappedToLatticeField = false;
            }
            if(savedObj.userField === fieldsMapped[mapped].userField){
                fieldsMapped[mapped].mappedField = savedObj.mappedField;
                fieldsMapped[mapped].mappedToLatticeField = isFieldPartOfSchema(entity, savedObj.mappedField);
                if(savedObj.cdlExternalSystemType){
                    fieldsMapped[mapped].cdlExternalSystemType = savedObj.cdlExternalSystemType;
                }
            }
        });
    }
    
    this.updateDocumentMapping = function(entity, savedObj, fieldsMapping){
        if(savedObj && fieldsMapping){
            var keysSaved = Object.keys(savedObj);
            
            keysSaved.forEach(function(keySaved){
                setMapping(entity, savedObj[keySaved], fieldsMapping);
            });
        }
    };
});