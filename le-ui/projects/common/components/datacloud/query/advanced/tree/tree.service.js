angular.module('common.datacloud.query.builder.tree.service', [])
    .service('QueryTreeService', function (QueryTreeAccountEntityService, QueryTreePurchaseHistoryService) {
        console.log('TEST service');
        var QueryTreeService = this;
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
            'NOT_EQUAL': 'is not equal to',
            'GREATER_THAN': 'is greater than',
            'GREATER_OR_EQUAL': 'is greater than or equal to',
            'LESS_THAN': 'is less than',
            'LESS_OR_EQUAL': 'is less than or equal to',
            'GTE_AND_LTE': 'is greater or equal and lesser or equal',
            'GTE_AND_LT': 'is greater or equal and less than',
            'GT_AND_LTE': "is greater than and lesser or equal",
            'GT_AND_LT': "is greater than and less than",
            'IN_COLLECTION': 'in collection',
            'CONTAINS': 'contains',
            'NOT_CONTAINS': 'not contains',
            'STARTS_WITH': 'starts with'
        };
        this.numerical_operations = {
            'IS_NULL': 'Empty',
            'IS_NOT_NULL': 'Not Empty',
            'EQUAL': 'Equal',
            'NOT_EQUAL': 'Not Equal',
            'GREATER_THAN': 'Greater Than',
            'GREATER_OR_EQUAL': 'Greater or Equal',
            'LESS_THAN': 'Less Than',
            'LESS_OR_EQUAL': 'Lesser or Equal',
            'GTE_AND_LTE': '>= and <=',
            'GTE_AND_LT': '>= and <',
            'GT_AND_LTE': "> and <=",
            'GT_AND_LT': "> and <"
        },
            this.enum_operations = {
                'EQUAL': 'Is Equal To',
                'NOT_EQUAL': 'Does Not Equal'
            };
        this.no_inputs = [
            'IS_NULL',
            'IS_NOT_NULL'
        ];
        this.two_inputs = [
            'GTE_AND_LTE',
            'GTE_AND_LT',
            'GT_AND_LTE',
            'GT_AND_LT'
        ];

        /**
         * Return the service based on the Entity type
         * 'Account', 'Contact', 'PurchaseHistory'
         * @param {*} entity 
         */
        function getService(entity) {
            switch (entity) {
                case 'Account':
                case 'Contact': {
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
            }
            else {
                console.warning('Service not implemented');
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
                console.warning('Service not implemented');
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
                console.warning('Service not implemented');
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
                console.warning('Service not implemented');
                return 'Uknown';
            }
        }

    })
    .service('QueryTreeAccountEntityService', function () {

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

            switch (type) {
                case 'Boolean': return cmpMap[bucketRestriction.bkt.Vals[0] || ''];
                case 'Numerical': return cmpMap[bucketRestriction.bkt.Cmp];
                case 'Enum': return cmpMap[bucketRestriction.bkt.Cmp];
                default: return 'has a value of';
            }
        }

        function getBooleanValue(bucketRestriction) {
            if (bucketRestriction.bkt.Vals[0] === 'Yes') {
                return 'True';
            } if (bucketRestriction.bkt.Vals[0] === 'No') {
                return 'False';
            }
            return 'Empty';
        }

        function getNumericalValue(bucketRestriction, position) {
            return bucketRestriction.bkt.Vals[position];
        }

        function getEnumValues(bucketRestriction) {
            return bucketRestriction.bkt.Vals;
        }

        this.getOperationValue = function (bucketRestriction, operatorType, position) {
            console.log('Operation Value', operatorType, position);
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
                default: return 'Unknown';
            }
        }


    })
    .service('QueryTreePurchaseHistoryService', function () {

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

            switch (type) {
                case 'TimeSeries': {
                    var txn = bucketRestriction.bkt.Txn;
                    var cmp = '';
                    if (txn.Amt) {
                        cmp = txn.Amt.Cmp;
                    } else {
                        cmp = txn.Time.Cmp;
                    }
                    return cmpMap[cmp];
                };
                default: return 'has a value of';
            }
        }

        function getBooleanValue(bucketRestriction) {
            if (bucketRestriction.bkt.Vals[0] === 'Yes') {
                return 'True';
            } if (bucketRestriction.bkt.Vals[0] === 'No') {
                return 'False';
            }
            return 'Empty';
        }

        function getNumericalValue(bucketRestriction, position) {
            return bucketRestriction.bkt.Vals[position];
        }

        function getEnumValues(bucketRestriction) {
            return bucketRestriction.bkt.Vals;
        }

        this.getOperationValue = function (bucketRestriction, operatorType, position) {
            console.log('Operation Value', operatorType, position);
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
                default: return 'Unknown';
            }
        }


    });