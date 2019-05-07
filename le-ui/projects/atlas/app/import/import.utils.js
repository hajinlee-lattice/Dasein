/**
 * Account:[
 *  {name: 'the_name', fieldType: 'TEXT', requiredIfNoField: boolean/null, requiredType:'Required'/'NotRequired'}
 * ]
 */
angular.module('lp.import.utils', ['mainApp.core.redux'])
.service('ImportUtils', function(ReduxService){
    var ImportUtils = this;
    ImportUtils.ACCOUNT_ENTITY = 'Account';
    ImportUtils.uniqueIds = { Account : {AccountId: 'AccountId'}, Contact: {ContactId: 'ContactId', AccountId: 'AccountId'}};
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
    
    this.getFieldFromLaticeSchema = function(entity, name){
        var entity = latticeSchema[entity];
        let field = entity.map[name];
        return field;
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
    this.updateLatticeDateField = (fieldMapped, dateObject) => {
        // console.log('UPDATE TO ==> ', dateObject);
        fieldMapped.dateFormatString = dateObject.dateFormatString ? dateObject.dateFormatString : null;
        fieldMapped.timeFormatString = dateObject.timeFormatString ? dateObject.timeFormatString : null;
        fieldMapped.timezone = dateObject.timezone ? dateObject.timezone : null;
        // console.log(fieldMapped);
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
            delete fieldMapped.dateFormatString;
            delete fieldMapped.timeFormatString;
            delete fieldMapped.timezone;
        }else{
            fieldMapped.dateFormatString = newTypeObj.dateFormatString;
            fieldMapped.timeFormatString = newTypeObj.timeFormatString;
            fieldMapped.timezone = newTypeObj.timezone;
        }
    }
    
    function removeUniqueIdsMapped( entity, fieldsMapped){
        let unique = [];
        if(fieldsMapped){
            fieldsMapped.forEach((fieldMapped, index) => {
                let entityUniqueIds = ImportUtils.uniqueIds[entity];
                if(entityUniqueIds && entityUniqueIds[fieldMapped.mappedField] && !isUniqueIdAlreadyAdded(unique, fieldMapped.mappedField, fieldMapped.userField)){
                    unique.push(Object.assign({}, fieldMapped));
                    fieldsMapped.splice(index,1);
                }
            });
        }
        return unique;
    }

    function setMapping(entity, savedObj, fieldsMapped){
        // let originalMapping = fieldsMapping ? fieldsMapping : {};
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
                updateFieldDate(fieldsMapped[mapped], savedObj);
            }
            
        });
    }
    
    function updateUniqueIdMapping(uniqueIdsList, obj){
        uniqueIdsList.forEach(uniqueId => {
            if(uniqueId.mappedField == obj.mappedField){
                uniqueId.userField = obj.userField;
                return;
            }
        });
    }
    function updateUniqueIdsMapping(entity, savedObj, uniquiIdslist){
        Object.keys(savedObj).forEach(index => {
            if(ImportUtils.uniqueIds[entity] && ImportUtils.uniqueIds[entity][savedObj[index].mappedField]){
                updateUniqueIdMapping(uniquiIdslist, savedObj[index]);
            }
        });

    }
    function isUniqueIdAlreadyAdded(uniqueIdsList, mappedField, userField){
        let already = false;
        uniqueIdsList.forEach((element) => {
            if(element.mappedField == mappedField && element.userField == userField){
                already = true;
                return;
            }
        });
        return already;
    }
    this.updateFormatSavedObj = (savedObj, field) => {
        var keysSaved = Object.keys(savedObj);
        keysSaved.forEach(function(keySaved){
            let saved = savedObj[keySaved];
            if(saved.mappedField == field.name){
                saved.dateFormatString = field.dateFormatString;
                saved.timeFormatString = field.timeFormatString;
                saved.timezone = field.timezone;
                return;
            }
        });
        return savedObj;
    }
    this.updateDocumentMapping = function(entity, savedObj, fieldsMapping){
        if(savedObj && fieldsMapping){
            var keysSaved = Object.keys(savedObj);
            let copyUniqueIds = removeUniqueIdsMapped(entity, fieldsMapping);
            updateUniqueIdsMapping(entity,savedObj,copyUniqueIds);
            keysSaved.forEach(function(keySaved){
                setMapping(entity, savedObj[keySaved], fieldsMapping);
            });
            let ret =  fieldsMapping.concat(copyUniqueIds);
            // console.log(ret);
            return ret;
        }else{
            return fieldsMapping;
        }
    };
});