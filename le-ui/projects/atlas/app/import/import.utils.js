import  { underscore } from 'common/vendor.index';
/**
 * Account:[
 *  {name: 'the_name', fieldType: 'TEXT', requiredIfNoField: boolean/null, requiredType:'Required'/'NotRequired'}
 * ]
 */
angular.module('lp.import.utils', ['mainApp.core.redux'])
.service('ImportUtils', function($state){
    var ImportUtils = this;
    ImportUtils.ACCOUNT_ENTITY = 'Account';
    ImportUtils.uniqueIds = { Account : {AccountId: 'AccountId', CustomerAccountId:'CustomerAccountId'}, Contact: {ContactId: 'ContactId', AccountId: 'AccountId', CustomerContactId: 'CustomerContactId'}};
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

    this.remapTypes = function(fieldsMapped, newTypesObj, entity){
        var userFields = Object.keys(newTypesObj);
        userFields.forEach(function(userFieldName){
            updateUserFieldType(fieldsMapped,userFieldName, newTypesObj[userFieldName], entity);
        });
    };
    this.updateLatticeDateField = (fieldMapped, dateObject) => {
        // console.log('UPDATE TO ==> ', dateObject);
        fieldMapped.dateFormatString = dateObject.dateFormatString ? dateObject.dateFormatString : null;
        fieldMapped.timeFormatString = dateObject.timeFormatString ? dateObject.timeFormatString : null;
        fieldMapped.timezone = dateObject.timezone ? dateObject.timezone : null;
        // console.log(fieldMapped);
    };

    function updateUserFieldType(fieldsMapped, userFieldName, newTypeObj, entity){
        fieldsMapped.forEach(function(fieldMapped){
            if(fieldMapped.userField == userFieldName){
                fieldMapped.fieldType = newTypeObj.type;
                updateFieldDate(fieldMapped, newTypeObj, entity);
                return;
            }
        });
    }

    function updateFieldDate(fieldMapped, newTypeObj, entity) {
        if(newTypeObj.type){
            if(newTypeObj.type !== 'DATE'){
                delete fieldMapped.dateFormatString;
                delete fieldMapped.timeFormatString;
                delete fieldMapped.timezone;
            }else{
                fieldMapped.dateFormatString = newTypeObj.dateFormatString;
                fieldMapped.timeFormatString = newTypeObj.timeFormatString;
                fieldMapped.timezone = newTypeObj.timezone;
            }
        }else{
            
            let newMapped = newTypeObj.mappedField;
            if(!newMapped){
                return;
            }
            fieldMapped.dateFormatString = newTypeObj.dateFormatString;
            fieldMapped.timeFormatString = newTypeObj.timeFormatString;
            fieldMapped.timezone = newTypeObj.timezone;
            let toLattice = fieldMapped.mappedToLatticeField;
            if(toLattice == true){
                let field = ImportUtils.getFieldFromLaticeSchema(entity, newMapped);
                fieldMapped.fieldType = field.fieldType;
                return;
            }else{
                let redux = $state.get('home.import').data.redux;
                let field = redux.store.fieldMappings.map[newMapped]
                if(field){
                    fieldMapped.fieldType = field.fieldType;
                }
            }
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
                if(savedObj.IdType || savedObj.SystemName){
                    fieldsMapped[mapped].idType = savedObj.IdType;
                    fieldsMapped[mapped].systemName = savedObj.SystemName;
                }
                updateFieldDate(fieldsMapped[mapped], savedObj, entity);
            }
            
        });
    }
    
    function mapUnmapUniqueId(fieldsMapping, uniqueId, fieldName, unmap, mapToLatticeId, IdType){
        if(uniqueId && fieldName){
            let keys = Object.keys(fieldsMapping);
            for(var i = 0; i < keys.length; i++){
                let field = fieldsMapping[keys[i]];
                if(field.userField == fieldName){
                    switch(unmap){
                        case true:
                            if(field.mappedField == uniqueId){
                                field.mappedToLatticeField = false;
                                field.mapToLatticeId = false;
                                field.idType = null;
                                delete field.mappedField;
                            }
                            break;
                        case false:
                            if(!field.fieldMapped){
                                field.mappedToLatticeField = true;
                                field.mapToLatticeId = mapToLatticeId ? mapToLatticeId : false;
                                field.mappedField = uniqueId;
                                field.idType = IdType;
                            }
                            break;
                    }
                    return;
                }
            }
        }
    }
    function updateUniqueIdsMapping(entity, fieldsMapping, savedObj, uniquiIdslist){
        Object.keys(savedObj).forEach(index => {
            let saved = savedObj[index];
            if(saved.originalUserField && saved.append != true){
                mapUnmapUniqueId(fieldsMapping, saved.mappedField, saved.originalUserField, true, false, null);
                mapUnmapUniqueId(fieldsMapping, saved.mappedField, saved.userField, false, saved.mapToLatticeId, saved.IdType);
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
            updateUniqueIdsMapping(entity, fieldsMapping, savedObj);
            let copyUniqueIds = removeUniqueIdsMapped(entity, fieldsMapping);
            keysSaved.forEach(function(keySaved){
                setMapping(entity, savedObj[keySaved], fieldsMapping);
            });
            let ret =  fieldsMapping.concat(copyUniqueIds);
            return ret;
        }else{
            return fieldsMapping;
        }
    };
});