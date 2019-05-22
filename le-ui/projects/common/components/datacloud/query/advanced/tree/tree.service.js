angular.module('common.datacloud.query.builder.tree.service', [
    'common.datacloud.query.builder.tree.purchasehistory.service'
])
    .service('QueryTreeService', function (
        $q, $http, QueryTreeAccountEntityService, QueryTreePurchaseHistoryService, QueryService
    ) {
        // console.log('TEST service');
        var QueryTreeService = this;
        /**
         * This object is used to define the association among treeMode and the entity
         */
        this.treeMapping = {
            'rating': 'account',
            'purchasehistory': 'account',
            'account': 'account',
            'contact': 'contact',
            'curatedaccount': 'account'

        };
        this.transactionMap = {
            'EVER': 'ever',
            'IN_CURRENT_PERIOD': 'current',
            'WITHIN': 'previous',
            'PRIOR_ONLY': 'only prior to last',
            'BETWEEN': 'between last',
            'BETWEEN_DATE': 'between',
            'BEFORE': 'before',
            'AFTER': 'after'
        };
        this.cmpMap = {
            "Yes": "is",
            "No": "is",
            "": "is",
            "is": "is",
            "empty": "is empty",
            "between": "is between",
            'IS_NULL': 'is empty',
            'IS_NOT_NULL': 'is present',
            'EQUAL': 'is equal to',
            'NOT_EQUAL': 'is not',
            'GREATER_THAN': 'is greater than',
            'GREATER_OR_EQUAL': 'is greater than or equal to',
            'LESS_THAN': 'is less than',
            'LESS_OR_EQUAL': 'is less than or equal to',
            'GTE_AND_LTE': 'is greater than or equal and lesser than or equal',
            'GTE_AND_LT': 'is between',
            'GT_AND_LTE': "is greater than and lesser or equal",
            'GT_AND_LT': "is greater than and less than",
            'IN_COLLECTION': 'is equal to',
            'NOT_IN_COLLECTION': 'is not',
            'CONTAINS': 'contains',
            'NOT_CONTAINS': 'not contains',
            'STARTS_WITH': 'starts with',
            'IN_CURRENT_PERIOD': 'current',
            'EVER': 'ever',
            'Any': 'any',
            'BEFORE': 'before',
            'BETWEEN': 'between',
            'AFTER': 'after',
            'BETWEEN_LT': 'between last',
            'PREVIOUS': 'previous',
            'PRIOR_ONLY_LT': 'only prior to last',
            'ENDS_WITH': 'ends with',
            'WITHIN': 'previous',
            'PRIOR_ONLY': 'only prior to last',
            'BETWEEN_DATE': 'between'
        };
        this.numerical_operations = {
            'IN_COLLECTION': 'equal to',
            'NOT_IN_COLLECTION': 'not equal to',
            'GREATER_THAN': 'greater than',
            'GREATER_OR_EQUAL': 'greater than or equal to',
            'LESS_THAN': 'less than',
            'LESS_OR_EQUAL': 'less than or equal to',
            'GTE_AND_LT': 'between',
            'IS_NULL': 'is empty',
            'IS_NOT_NULL': 'is present'
        };
        this.enum_operations = {
            'EQUAL': 'is equal to',
            'NOT_EQUAL': 'is not',
            'IN_COLLECTION': 'is equal to',
            'NOT_IN_COLLECTION': 'is not'
        };
        this.string_operations = {
            'IN_COLLECTION': 'equal to',
            'NOT_IN_COLLECTION': 'not equal to',
            'STARTS_WITH': 'starts with',
            'ENDS_WITH': 'ends with',
            'CONTAINS': 'contains',
            'NOT_CONTAINS': 'does not contain',
            'IS_NULL': 'is empty',
            'IS_NOT_NULL': 'is present'
        };
        this.no_inputs = [
            'IS_NULL',
            'IS_NOT_NULL'
        ];
        this.two_inputs = [
            'GTE_AND_LT'
        ];
        this.numerical_labels = {
            'EQUAL': '=',
            'NOT_EQUAL': '!=',
            'GREATER_THAN': '>',
            'GREATER_OR_EQUAL': '>=',
            'LESS_THAN': '<',
            'LESS_OR_EQUAL': '<=',
            'IS_NULL': 'is empty',
            'IS_NOT_NULL': 'is present'
        };

        this.prevBucketCountAttr = null;

        /**
         * Return the service based on the Entity type
         * 'Account', 'Contact', 'PurchaseHistory'
         * @param {*} entity 
         */
        function getService(entity) {
            switch (entity) {
                case 'LatticeAccount':
                case 'Account':
                case 'Contact':
                case 'CuratedAccount':
                case 'Rating': {
                    return QueryTreeAccountEntityService;
                };
                case 'PurchaseHistory': {
                    return QueryTreePurchaseHistoryService;
                };
                default: return null;
            }
        }

        /**
         * Retrives from attr the Entity
         * @param {*} bucketRestriction 
         */
        function getEntity(bucketRestriction) {
            if (bucketRestriction) {
                var attr = bucketRestriction.attr;
                if (attr) {
                    var dot = attr.indexOf('.');
                    var entity = attr.slice(0, dot);
                    return entity;
                } else {
                    return 'Unkown';
                }
            } else {
                console.warn('Bucket Restriction not existing', bucketRestriction);
                return 'Unkown';
            }
        }

        this.showEmptyOption = function (bucketRestriction) {
            if ('PurchaseHistory' === getEntity(bucketRestriction)) {
                return false;
            } else {
                return true;
            }
        }

        /**
         * Return if a type ('Boolean', 'Numerical', 'Enum') can be shown or not
         * @param {*} bucketRestriction 
         * @param {*} type 
         * @param {*} typeToShow 
         */
        this.showType = function (bucketRestriction, type, typeToShow) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            if (service) {
                return service.showType(bucketRestriction, type, typeToShow);
            } else {
                console.warn('Service not implemented');
                return false;
            }
        }

        /**
         * Return if the second value (To) can be shown or not
         * @param {*} bucketRestriction 
         */
        this.showTo = function (bucketRestriction) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            if (service) {
                return service.showTo(bucketRestriction, this.two_inputs);
            } else {
                console.warn('Service not implemented');
                return false;
            }
        }
        /**
         * Return the operation label (Ex. 'is equal to' 'is less than')
         * based on the Entity
         * @param {*} type 
         * @param {*} bucketRestriction 
         */
        this.getOperationLabel = function (type, bucketRestriction) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            if (service) {
                return service.getOperationLabel(QueryTreeService.cmpMap, type, bucketRestriction);
            } else {
                console.warn('Service not implemented');
                return 'has a value of';
            }
        }
        /**
         * Return the value of the bucket restriction
         * @param {*} bucketRestriction 
         * @param {*} operatorType 
         * @param {*} position 
         */
        this.getOperationValue = function (bucketRestriction, operatorType, position) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            if (service) {
                return service.getOperationValue(bucketRestriction, operatorType, position);
            } else {
                console.warn('Service not implemented');
                return 'Unknown';
            }
        }

        this.getAttributeRules = function (bucketRestriction, bkt, bucket, isSameAttribute) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            return service.getAttributeRules(bkt, bucket, isSameAttribute);
        }

        this.getValue = function (bucketRestriction, type, position, subType) {
            if (subType != undefined) {
                var entity = getEntity(bucketRestriction);
                var service = getService(entity);
                return service.getValue(bucketRestriction, type, position, subType);
            } else {
                return this.getBktValue(bucketRestriction, position);
            }
        }

        this.getValues = function (bucketRestriction, type, subType) {
            if (subType === undefined) {
                return this.getBktVals(bucketRestriction, type);
            } else {
                var entity = getEntity(bucketRestriction);
                var service = getService(entity);
                return service.getValues(bucketRestriction, type, subType);
            }
        }

        this.getBktVals = function (bucketRestriction, type) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            return service.getBktVals(bucketRestriction, type);
        }

        this.isBucketUsed = function (bucket) {
            // console.log('Service ', bucket);
            // return bucket.ignored;
            var entity = getEntity(bucket);
            var service = getService(entity);
            return service.isBucketUsed(bucket);
        }

        this.hasInputs = function (type, bucketRestriction) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            var cmpModel = bucketRestriction.bkt.Cmp;
            if (service) {
                switch (type) {
                    case 'Enum':
                        cmpModel = service.getEnumCmpModel(bucketRestriction);
                        break;
                    case 'String':
                        cmpModel = service.getStringCmpModel(bucketRestriction);
                        break;
                    case 'Numerical':
                        cmpModel = service.getNumericalCmpModel(bucketRestriction);
                        break;
                }
                return this.no_inputs.indexOf(cmpModel) == -1;
            } else {
                console.warn('getCmpModel() service not implemented');
            }

        }

        //***************** Editing ************************************/


        /**
         * Return the var for ng-model in a boolean bucket restriction
         * @param {*} bucketRestriction 
         */
        this.getBooleanModel = function (bucketRestriction) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            if (service) {
                return service.getBooleanModel(bucketRestriction);
            } else {
                console.warn(' getBooleanModel() Service not implemented');
            }
        }

        this.getStringCmpModel = function (bucketRestriction) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            if (service) {
                return service.getStringCmpModel(bucketRestriction);
            } else {
                console.warn(' getStringCmpModel() Service not implemented');
            }
        }

        this.getEnumCmpModel = function (bucketRestriction) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            if (service) {
                return service.getEnumCmpModel(bucketRestriction);
            } else {
                console.warn(' getEnumCmpModel() Service not implemented');
            }
        }

        this.getNumericalCmpModel = function (bucketRestriction) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            if (service) {
                return service.getNumericalCmpModel(bucketRestriction);
            } else {
                console.warn(' getNumericalCmpModel() Service not implemented');
            }
        }

        /**
        * Change a boolean value for a Boolean restriction
        * @param {*} bucketRestriction 
        */
        this.changeBooleanValue = function (bucketRestriction, booleanValue) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            if (service) {
                service.changeBooleanValue(bucketRestriction, booleanValue);
            } else {
                console.warn(' changeBooleanValue() Service not implemented');
            }
        }
        this.changeCmpValue = function (bucketRestriction, value) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            if (service) {
                return service.changeCmpValue(bucketRestriction, value);
            } else {
                console.warn(' changeCmpValue() Service not implemented');
            }
        }
        this.changeVals = function (bucketRestriction, value) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            if (service) {
                service.changeVals(bucketRestriction, value);
            } else {
                console.warn(' changeVals() Service not implemented');
            }
        }
        this.changeNumericalCmpValue = function (bucketRestriction, value) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            if (service) {
                QueryTreeService.changeBktValsSize(bucketRestriction, value);
                return service.changeNumericalCmpValue(bucketRestriction, value);
            } else {
                console.warn(' changeNumericalCmpValue() Service not implemented');
            }
        }

        this.changeBktValsSize = function (bucketRestriction, value) {
            if (QueryTreeService.two_inputs.indexOf(value) < 0 && bucketRestriction.bkt.Vals.length == 2) {
                bucketRestriction.bkt.Vals.splice(1, 1);
            }
        }

        this.changeBktValue = function (bucketRestriction, value, position) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            if (service) {
                return service.changeBktValue(bucketRestriction, value, position);
            } else {
                console.warn(' changeNumericalCmpValue() Service not implemented');
            }
        }

        this.changeValue = function (bucketRestriction, type, value, position, subType) {
            if (subType === undefined) {
                this.changeBktValue(bucketRestriction, value, position);
            } else {
                var entity = getEntity(bucketRestriction);
                var service = getService(entity);
                service.changeValue(bucketRestriction, type, value, position, subType);
            }
        }

        this.changeCmp = function (bucketRestriction, type, value, subType) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            if (service) {
                return service.changeCmp(bucketRestriction, type, value, subType);
            } else {
                console.warn(' changeNumericalCmpValue() Service not implemented');
            }
        }

        this.changeTimeframePeriod = function (bucketRestriction, type, value) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            if (service) {
                return service.changeTimeframePeriod(bucketRestriction, type, value);
            } else {
                console.warn(' changeNumericalCmpValue() Service not implemented');
            }
        }

        this.removeKey = function (bucketRestriction, type, subType) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            if (service) {
                return service.removeKey(bucketRestriction, type, subType);
            } else {
                console.warn(' removeKey() Service not implemented');
            }
        }
        this.resetBktValues = function (bucketRestriction, type, subType) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            if (service) {
                return service.resetBktValues(bucketRestriction, type, subType);
            } else {
                console.warn(' resetBktValues() Service not implemented');
            }
        }

        this.getBktValue = function (bucketRestriction, position) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            if (service) {
                return service.getBktValue(bucketRestriction, position);
            } else {
                console.warn(' getBktValue() Service not implemented');
            }
        }

        this.getCubeBktList = function (bucketRestriction, cube) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            if (service) {
                return service.getCubeBktList(cube);
            } else {
                console.warn(' getCubeBktList() Service not implemented');
            }
        }

        this.setPickerObject = function (attribute) {
            this.picker_object = attribute;
        }

        this.getPickerObject = function (attribute) {
            return this.picker_object;
        }

        this.getPickerCubeData = function (entity, fieldname) {
            var deferred = $q.defer();

            $http({
                method: 'GET',
                url: '/pls/datacollection/statistics/attrs/' + entity + '/' + fieldname
            }).then(function (result) {
                deferred.resolve(result);
            });

            return deferred.promise;
        }

        this.updateBucketCount = function (bucketRestriction, segmentName) {
            var deferred = $q.defer();

            var segment = {
                "free_form_text_search": ""
            };
            if (segmentName) {
                segment.preexisting_segment_name = segmentName;
                // segment.contact_restriction = {};
            }
            this.treeMode = bucketRestriction.attr.split('.')[0].toLowerCase();
            //This call is done to associate some mode to account
            this.treeMode = QueryTreeService.treeMapping[this.treeMode];
            segment[this.treeMode + '_restriction'] = {
                "restriction": {
                    "bucketRestriction": angular.copy(bucketRestriction)
                }
            };
            QueryService.GetCountByQuery(
                this.treeMode + 's',
                segment,
                bucketRestriction.attr == this.prevBucketCountAttr
            ).then(function (result) {
                deferred.resolve(result);
            });

            this.prevBucketCountAttr = bucketRestriction.attr;

            return deferred.promise;
        }


        this.getCmp = function (bucketRestriction, type) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            if (service) {
                return service.getCmp(bucketRestriction, type);
            } else {
                console.warn(' getCmp() Service not implemented');
            }
        }

        this.getCmp = function (bucketRestriction, type, subType) {
            if (subType === undefined) {
                return this.getCmp(bucketRestriction, type);
            } else {
                var entity = getEntity(bucketRestriction);
                var service = getService(entity);
                if (service) {
                    return service.getCmp(bucketRestriction, type, subType);
                } else {
                    console.warn(' getCmp() Service not implemented');
                }
            }
        }

        this.getPeriodValue = function (bucketRestriction, type, subType) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            return service.getPeriodValue(bucketRestriction, type, subType);
        }

    })
    .service('QueryTreeAccountEntityService', function () {
        function setValsBasedOnPosition(cmp, valsArray, position, value) {
            switch (cmp) {
                case 'GTE_AND_LT': {
                    valsArray[position] = value;
                    break;
                }
                default: {
                    valsArray[0] = value;
                }
            }
        }
        function getValsBasedOnPosition(cmp, valsArray, position) {
            switch (cmp) {
                case 'GTE_AND_LT': {
                    return valsArray[position];
                    break;
                }
                case 'GREATER_THAN':
                case 'GREATER_OR_EQUAL':
                case 'NOT_EQUAL':
                case 'EQUAL': {
                    if (position == valsArray.length - 1 || valsArray.length == 0) {
                        return valsArray[valsArray.length - 1];
                    } else {
                        return null;
                    }
                    break;
                }
                case 'LESS_THAN':
                case 'LESS_OR_EQUAL': {
                    if (position == valsArray.length - 1 || valsArray.length == 0) {
                        return null;
                    } else {
                        return valsArray[valsArray.length - 1];
                    }
                    break;
                    ;
                }
                default: {
                    return null;
                }
            }
        }

        this.showType = function (bucketRestriction, type, typeToShow) {
            switch (typeToShow) {
                case 'Numerical': {
                    if (typeToShow === type) {
                        return true;
                    } else {
                        return false;
                    }
                };
                case 'Boolean': {
                    if (type === typeToShow) {
                        return true;
                    } else {
                        return false;
                    }
                };
                case 'Enum': {
                    if (type === typeToShow) {
                        return true;
                    } else {
                        return false;
                    }
                };
                case 'Date': {
                    return typeToShow === type;
                };
                case 'String': {
                    return type === typeToShow;
                }
                default: return false;
            }

        }

        this.showTo = function (bucketRestriction, two_inputs) {
            if (two_inputs.indexOf(bucketRestriction.bkt.Cmp) >= 0) {
                return true;
            } else {
                return false;
            }
        }
        /**
         * Return the operation label for and Account or Contacts Entity
         * @param {*} cmpMap 
         * @param {*} type 
         * @param {*} bucketRestriction 
         */
        this.getOperationLabel = function (cmpMap, type, bucketRestriction) {
            if (!bucketRestriction.bkt) {
                return;
            }
            var cmp = bucketRestriction.bkt.Cmp;
            var vals = bucketRestriction.bkt.Vals;

            switch (type) {
                case 'Boolean':
                    return cmpMap[bucketRestriction.bkt.Vals[0] || ''];
                case 'Numerical':
                    return cmpMap[cmp];
                case 'String':
                case 'Enum':
                    switch (cmp) {
                        case 'EQUAL':
                        case 'IN_COLLECTION':
                            return cmpMap['IN_COLLECTION'];
                        case 'NOT_EQUAL':
                        case 'NOT_IN_COLLECTION':
                            return cmpMap['NOT_IN_COLLECTION'];
                        default:
                            return cmpMap[cmp];
                    }
                case 'Date':
                    return 'in timeframe';
                default:
                    return 'has a value of';
            }
        }

        function getBooleanValue(bucketRestriction) {
            if (bucketRestriction.bkt.Vals && bucketRestriction.bkt.Vals[0] === 'Yes') {
                return 'True';
            } if (bucketRestriction.bkt.Vals && bucketRestriction.bkt.Vals[0] === 'No') {
                return 'False';
            }
            return 'Empty';
        }

        function getNumericalValue(bucketRestriction, position) {
            return bucketRestriction.bkt.Vals[position];
        }

        function getEnumValues(bucketRestriction) {
            return bucketRestriction.bkt.Vals || [];
        }

        function getStringValue(bucketRestriction) {
            if (bucketRestriction.bkt.Cmp == 'IS_NULL' || bucketRestriction.bkt.Cmp == 'IS_NOT_NULL') {
                return '';
            }
            return bucketRestriction.bkt.Vals && bucketRestriction.bkt.Vals.length > 0 ? bucketRestriction.bkt.Vals : 'any (*)';
        }
        function getDateValue(bucketRestriction) {
            if (bucketRestriction.bkt.Fltr.Cmp == 'IS_NULL' || bucketRestriction.bkt.Fltr.Cmp == 'IS_NOT_NULL') {
                return '';
            }
            return bucketRestriction.bkt.Fltr.Vals && bucketRestriction.bkt.Fltr.Vals[0] ? bucketRestriction.bkt.Fltr.Vals[0] : 'any (*)';
        }

        this.getOperationValue = function (bucketRestriction, operatorType, position) {
            // console.log('Operation Value', operatorType, position);
            switch (operatorType) {
                case 'Boolean': {
                    return getBooleanValue(bucketRestriction);
                };
                case 'Numerical': {
                    return getNumericalValue(bucketRestriction, position);
                };
                case 'Enum': {
                    return getEnumValues(bucketRestriction);
                };
                case 'String': {
                    return getStringValue(bucketRestriction);
                };
                case 'Date': {
                    let tmp = getDateValue(bucketRestriction);
                    console.log('TMP ', tmp);
                    return tmp;
                }
                default: return 'Unknown';
            }
        }

        this.getAttributeRules = function (bkt, bucket, isSameAttribute) {
            // console.log('Account', bucket, bkt);
            var isSameBucket = true;
            if (bucket && bucket.Vals !== undefined && bucket.Vals != null && bkt.Vals !== undefined && bkt.Vals != null) {
                isSameBucket = bkt.Vals[0] == bucket.Vals[0] && bkt.Vals[1] == bucket.Vals[1] && bkt.Cmp == bucket.Cmp;
            } else if (bucket && bucket.Fltr !== undefined && bucket.Fltr.Vals != null && bkt.Fltr && bkt.Fltr.Vals !== undefined && bkt.Fltr.Vals != null) {
                isSameBucket = bkt.Fltr.Vals[0] == bucket.Fltr.Vals[0] && bkt.Fltr.Vals[1] == bucket.Fltr.Vals[1] && bkt.Fltr.Cmp == bucket.Fltr.Cmp;
            }



            return isSameAttribute && isSameBucket;
        }

        this.isBucketUsed = function (bucket) {
            // return bucket.bkt && typeof bucket.bkt.Id == "number" || bucket.ignored !== true;
            return bucket.ignored !== true;
        }

        this.getBktVals = function (bucketRestriction, type) {
            return bucketRestriction.bkt && bucketRestriction.bkt.Vals;
        }

        this.getValue = function (bucketRestriction, type, position, subType) {
            console.log('getValue', bucketRestriction, type, position, subType)
        }

        this.getValues = function (bucketRestriction, type, subType) {
            switch (type) {
                case 'Date':
                    if (bucketRestriction.bkt.Fltr.Cmp == 'EVER') {
                        return [];
                    } else {
                        return bucketRestriction.bkt.Fltr.Vals;
                    }

                default: return [];
            }
        };

        //******************** Editing mode *********************************/
        this.changeBooleanValue = function (bucketRestriction, booleanValue) {
            bucketRestriction.bkt.Vals[0] = booleanValue.length ? booleanValue : null;
        }
        this.changeVals = function (bucketRestriction, value) {
            bucketRestriction.bkt.Vals = value;
        }
        this.changeCmpValue = function (bucketRestriction, value) {
            bucketRestriction.bkt.Cmp = value;
        }
        this.changeNumericalCmpValue = function (bucketRestriction, value) {
            bucketRestriction.bkt.Cmp = value;
        }

        this.changeBktValue = function (bucketRestriction, value, position) {
            setValsBasedOnPosition(bucketRestriction.bkt.Cmp, bucketRestriction.bkt.Vals, position, value);
            // bucketRestriction.bkt.Vals[position] = value;
        }

        this.changeValue = function (bucketRestriction, type, value, position, subType) { }

        this.changeTimeframePeriod = function (bucketRestriction, type, value) {
            switch (type) {
                case 'Date':
                    if (bucketRestriction) {
                        bucketRestriction.bkt.Fltr.Period = value.Period;
                        bucketRestriction.bkt.Fltr.Vals = value.Vals;
                    }
                    break;
            }
        }

        this.resetBktValues = function (bucketRestriction, type, subType) {
            bucketRestriction.bkt.Vals = [];
        }
        this.removeKey = function (bucketRestriction, type, subtype) {
            console.warn('Not implemented');
        }
        this.getBooleanModel = function (bucketRestriction) {
            return bucketRestriction.bkt.Vals[0];
        }
        this.getStringCmpModel = function (bucketRestriction) {
            return bucketRestriction.bkt.Cmp;
        }
        this.getEnumCmpModel = function (bucketRestriction) {
            return bucketRestriction.bkt.Cmp;
        }

        this.getNumericalCmpModel = function (bucketRestriction) {
            return bucketRestriction.bkt.Cmp;
        }

        this.getBktValue = function (bucketRestriction, position) {
            return getValsBasedOnPosition(bucketRestriction.bkt.Cmp, bucketRestriction.bkt.Vals, position);
            // return bucketRestriction.bkt.Vals[position];
        }

        this.getCubeBktList = function (cube) {
            return cube.Bkts.List;
        }

        this.getCmp = function (bucketRestriction, type, subType) {
            switch (type) {
                case 'Date':
                    return bucketRestriction.bkt.Fltr ? bucketRestriction.bkt.Fltr.Cmp : bucketRestriction.bkt.Cmp;
            }
            return '';
        }

        this.getPeriodValue = function (bucketRestriction, type, subtype) {
            switch (type) {
                case 'Date':
                    return bucketRestriction.bkt.Fltr ? bucketRestriction.bkt.Fltr.Period : bucketRestriction.bkt.Period;
            }
            return '';
        }

        this.changeCmp = function (bucketRestriction, type, value, subType) {

        }
    });