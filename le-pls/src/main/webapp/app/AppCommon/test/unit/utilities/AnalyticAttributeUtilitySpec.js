'use strict';

describe('AnalyticAttributeUtility Tests', function () {
    
    var resourceUtility,
        dateTimeFormatUtility,
        numberUtility,
        talkingPointMetadata,
        analyticAttributeUtility,
        sampleModelSummary,
        sampleContinuousData,
        sampleDiscreteData;
    
    beforeEach(function () {
        module('mainApp.appCommon.utilities.ResourceUtility');
        module('mainApp.appCommon.utilities.DateTimeFormatUtility');
        module('mainApp.appCommon.utilities.NumberUtility');
        module('mainApp.appCommon.utilities.AnalyticAttributeUtility');
        module('test.testData.AnalyticAttributeTestDataService');
        inject(['ResourceUtility', 'DateTimeFormatUtility', 'NumberUtility', 'AnalyticAttributeUtility', 'AnalyticAttributeTestDataService',
            function (ResourceUtility, DateTimeFormatUtility, NumberUtility, AnalyticAttributeUtility, AnalyticAttributeTestDataService) {
                resourceUtility = ResourceUtility;
                resourceUtility.configStrings = {
                    ANALYTIC_ATTRIBUTE_ALL_VALUES_LABEL: "All Values",
                    ANALYTIC_ATTRIBUTE_NULL_VALUE_LABEL: "Null",
                    ANALYTIC_ATTRIBUTE_CONTINUOUS_BETWEEN_LABEL: "Between {0} and {1}",
                    ANALYTIC_ATTRIBUTE_GREATER_THAN_LABEL: "Greater than {0}",
                    ANALYTIC_ATTRIBUTE_LESS_THAN_LABEL: "Less than {0}",
                    CURRENCY_SYMBOL: "$",
                    BOOLEAN_TRUE_DISPLAY_LABEL: "Yes",
                    BOOLEAN_FALSE_DISPLAY_LABEL: "No"
                };
                dateTimeFormatUtility = DateTimeFormatUtility;
                numberUtility = NumberUtility;
                analyticAttributeUtility = AnalyticAttributeUtility;
                sampleModelSummary = AnalyticAttributeTestDataService.GetSampleModelSummaryData();
                
                sampleContinuousData = {
                    AttributeID: "2",
                    AttributeName: "Attr4",
                    AttributeValue: "48"
                };
                
                sampleDiscreteData = {
                    AttributeID: "1",
                    AttributeName: "Attr1",
                    AttributeValue: "Attr1Value3"
                };
            }
        ]);
    });
    
    describe('FindAttributeMetadataData given null parameters', function () {
        var toReturn;
        
        it('should return null for a null model summary name', function () {
            toReturn = analyticAttributeUtility.FindAttributeMetadataData(null, sampleContinuousData);
            expect(toReturn).toBe(null);
        });
        
        it('should return null for null attribute data', function () {
            toReturn = analyticAttributeUtility.FindAttributeMetadataData(sampleModelSummary, null);
            expect(toReturn).toBe(null);
        });
    });
    
    describe('FindAttributeMetadataData given valid model summary and data', function () {
        var toReturn;
        
        var discreteMetadata = {
            ColumnName: "Attr1",
            ApprovedUsage: [
                "ModelAndAllInsights"
            ],
            Description: "Description1",
            DisplayName: "DisplayName1",
            Tags: [
                "External"
            ],
            DataType: "String"
        };
        
        var continuousMetadata = {
            ColumnName: "Attr4",
            ApprovedUsage: [
                "ModelAndAllInsights"
            ],
            Description: "Description4",
            DisplayName: "DisplayName4",
            Tags: [
                "Internal"
            ],
            DataType: "Int"
        };
        
        it('should return valid metadata for valid continuous data', function () {
            toReturn = analyticAttributeUtility.FindAttributeMetadataData(sampleModelSummary, sampleContinuousData);
            expect(toReturn).toEqual(continuousMetadata);
        });
        
        it('should return valid metadata for valid discrete data', function () {
            toReturn = analyticAttributeUtility.FindAttributeMetadataData(sampleModelSummary, sampleDiscreteData);
            expect(toReturn).toEqual(discreteMetadata);
        });
    });
    
    describe('FindAttributeBucket given null parameters', function () {
        var toReturn;
        
        it('should return null for a null model summary name', function () {
            toReturn = analyticAttributeUtility.FindAttributeBucket(null, sampleContinuousData);
            expect(toReturn).toBe(null);
        });
        
        it('should return null for null attribute data', function () {
            toReturn = analyticAttributeUtility.FindAttributeBucket(sampleModelSummary, null);
            expect(toReturn).toBe(null);
        });
    });
    
    describe('FindAttributeBucket given valid model summary and data', function () {
        var toReturn;
        
        var discreteBucket = {
            ColumnName: "Attr1Name3",
            Values: [
                "Attr1Value3"
            ],
            LowerInclusive: null,
            UpperExclusive: null
        };
        
        var continuousBucket = {
            ColumnName: "Attr4Name2",
            Values: [],
            LowerInclusive: 40,
            UpperExclusive: 60
        };
        
        var greaterThanContinuousBucket = {
            ColumnName: "Attr4Name4",
            Values: [],
            LowerInclusive: 80,
            UpperExclusive: null
        };
        
        var greaterThanContinuousData = {
            AttributeID: "3",
            AttributeName: "Attr4",
            AttributeValue: "100"
        };
        
        it('should return a bucket for valid continuous data', function () {
            toReturn = analyticAttributeUtility.FindAttributeBucket(sampleModelSummary, sampleContinuousData);
            expect(toReturn).toEqual(continuousBucket);
        });
        
        it('should return a bucket for valid discrete data', function () {
            toReturn = analyticAttributeUtility.FindAttributeBucket(sampleModelSummary, sampleDiscreteData);
            expect(toReturn).toEqual(discreteBucket);
        });
        
        it('should return a greater than bucket for valid continuous data', function () {
            toReturn = analyticAttributeUtility.FindAttributeBucket(sampleModelSummary, greaterThanContinuousData);
            expect(toReturn).toEqual(greaterThanContinuousBucket);
        });
    });
    
    describe('GetAttributeBucketName given invalid parameters', function () {
        var toReturn;
        
        it('should return an empty string if not given a bucket', function () {
            toReturn = analyticAttributeUtility.GetAttributeBucketName(null);
            expect(toReturn).toBe("");
        });
        
        var bucket = {
            ColumnName: "Attr1Name3",
            Values: [
                "Attr1Value3"
            ],
            LowerInclusive: null,
            UpperExclusive: null
        };
        
        it('should return the value if no metadata is passed in', function () {
            toReturn = analyticAttributeUtility.GetAttributeBucketName(bucket, null);
            expect(toReturn).toBe("Attr1Value3");
        });
    });
    
    describe('GetAttributeBucketName for a discrete value', function () {
        var toReturn;
        
        var bucket = {
            ColumnName: "Attr1Name3",
            Values: [
                "Attr1Value3"
            ],
            LowerInclusive: null,
            UpperExclusive: null
        };
        
        var metadata = {
            DataType: "String",
            FundamentalType: "enum"
        };
        
        it('should return the bucket name as the value', function () {
            toReturn = analyticAttributeUtility.GetAttributeBucketName(bucket, metadata);
            expect(toReturn).toBe("Attr1Value3");
        });
    });
    
    describe('GetAttributeBucketName for multiple discrete value', function () {
        var toReturn;
        
        var bucket = {
            ColumnName: "Attr1Name3",
            Values: [
                "Attr1Value3", 
                "Attr1Value4"
            ],
            LowerInclusive: null,
            UpperExclusive: null
        };
        
        var metadata = {
            DataType: "String",
            FundamentalType: "enum"
        };
        
        it('should return the bucket name as a comma separated list of the values', function () {
            toReturn = analyticAttributeUtility.GetAttributeBucketName(bucket, metadata);
            expect(toReturn).toBe("Attr1Value3, Attr1Value4");
        });
    });
    
    describe('GetAttributeBucketName for the null bucket', function () {
        var toReturn;
        
        var bucket = {
            ColumnName: "Attr1Name3",
            Values: [
                null
            ],
            LowerInclusive: null,
            UpperExclusive: null
        };
        
        var metadata = {
            DataType: "String",
            FundamentalType: "enum"
        };
        
        it('should return the bucket name as the null resource string', function () {
            toReturn = analyticAttributeUtility.GetAttributeBucketName(bucket, metadata);
            expect(toReturn).toBe("Null");
        });
    });
    
    describe('GetAttributeBucketName all values', function () {
        var toReturn;
        
        var bucket = {
            ColumnName: "Attr1Name3",
            Values: null,
            LowerInclusive: null,
            UpperExclusive: null
        };
        
        var metadata = {
            DataType: "String",
            FundamentalType: "enum"
        };
        
        it('should return the bucket name as the all values resource string', function () {
            toReturn = analyticAttributeUtility.GetAttributeBucketName(bucket, metadata);
            expect(toReturn).toBe("All Values");
        });
    });
    
    describe('GetAttributeBucketName continuous between', function () {
        var toReturn;
        
        var bucket = {
            ColumnName: "Attr1Name3",
            Values: null,
            LowerInclusive: 40,
            UpperExclusive: 60
        };
        
        var metadata = {
            DataType: "String",
            FundamentalType: "enum"
        };
        
        it('should return the bucket name as between LowerInclusive and UpperExclusive values', function () {
            toReturn = analyticAttributeUtility.GetAttributeBucketName(bucket, metadata);
            expect(toReturn).toBe("Between 40 and 60");
        });
    });
    
    describe('GetAttributeBucketName continuous greater than', function () {
        var toReturn;
        
        var bucket = {
            ColumnName: "Attr1Name3",
            Values: null,
            LowerInclusive: 40,
            UpperExclusive: null
        };
        
        var metadata = {
            DataType: "String",
            FundamentalType: "enum"
        };
        
        it('should return the bucket name as greater than LowerInclusive value', function () {
            toReturn = analyticAttributeUtility.GetAttributeBucketName(bucket, metadata);
            expect(toReturn).toBe("Greater than 40");
        });
    });
    
    describe('GetAttributeBucketName continuous less than', function () {
        var toReturn;
        
        var bucket = {
            ColumnName: "Attr1Name3",
            Values: null,
            LowerInclusive: null,
            UpperExclusive: 60
        };
        
        var metadata = {
            DataType: "String",
            FundamentalType: "enum"
        };
        
        it('should return the bucket name as less than UpperExclusive value', function () {
            toReturn = analyticAttributeUtility.GetAttributeBucketName(bucket, metadata);
            expect(toReturn).toBe("Less than 60");
        });
    });
    
    describe('FormatBucketValue given null parameters', function () {
        var toReturn;

        var metadata = {
            DataType: "String",
            FundamentalType: "enum"
        };
        
        it('should return null if the value is null', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(null, metadata);
            expect(toReturn).toBe(null);
        });
        
        it('should return the value if the metadata is null', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(40, null);
            expect(toReturn).toBe(40);
        });
        
        it('should return null if the value and the metadata are null', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(null, null);
            expect(toReturn).toBe(null);
        });
    });
    
    describe('FormatBucketValue given String value', function () {
        var toReturn;

        var metadata = {
            DataType: "String",
            FundamentalType: "enum"
        };
        
        it('should return a String if the metadata states it is a String type', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue("Test", metadata);
            expect(toReturn).toBe("Test");
        });
    });
    
    describe('FormatBucketValue given Int value', function () {
        var toReturn;

        var metadata = {
            DataType: "Int",
            FundamentalType: null
        };
        
        it('If value is less than 1 it should get rounded to 2 decimals places', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(0.025, metadata);
            expect(toReturn).toBe("0.03");
        });
        
        it('If value is greater than 1 it should only be rounded t o1 decimal place', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(1.61, metadata);
            expect(toReturn).toBe("1.6");
        });
        
        it('If value is less than 1000 then it should not be abbreviated', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(40, metadata);
            expect(toReturn).toBe(40);
        });
        
        it('If value is less than 1,000,000 then it should be abbreviated with a K', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(40000, metadata);
            expect(toReturn).toBe("40K");
        });
        
        it('If value is greater than or equal to 1,000,000 then it should be abbreviated with an M', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(4000000, metadata);
            expect(toReturn).toBe("4M");
        });
    });
    
    describe('FormatBucketValue given Integer value', function () {
        var toReturn;

        var metadata = {
            DataType: "Integer",
            FundamentalType: null
        };
        
        it('If value is less than 1 it should get rounded to 2 decimals places', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(0.025, metadata);
            expect(toReturn).toBe("0.03");
        });
        
        it('If value is greater than 1 it should only be rounded t o1 decimal place', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(1.61, metadata);
            expect(toReturn).toBe("1.6");
        });
        
        it('If value is less than 1000 then it should not be abbreviated', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(40, metadata);
            expect(toReturn).toBe(40);
        });
        
        it('If value is less than 1,000,000 then it should be abbreviated with a K', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(40000, metadata);
            expect(toReturn).toBe("40K");
        });
        
        it('If value is greater than or equal to 1,000,000 then it should be abbreviated with an M', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(4000000, metadata);
            expect(toReturn).toBe("4M");
        });
    });
    
    describe('FormatBucketValue given Double value', function () {
        var toReturn;

        var metadata = {
            DataType: "Double",
            FundamentalType: null
        };
        
        it('If value is less than 1 it should get rounded to 2 decimals places', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(0.025, metadata);
            expect(toReturn).toBe("0.03");
        });
        
        it('If value is greater than 1 it should only be rounded t o1 decimal place', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(1.61, metadata);
            expect(toReturn).toBe("1.6");
        });
        
        it('If value is less than 1000 then it should not be abbreviated', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(40, metadata);
            expect(toReturn).toBe(40);
        });
        
        it('If value is less than 1,000,000 then it should be abbreviated with a K', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(40000, metadata);
            expect(toReturn).toBe("40K");
        });
        
        it('If value is greater than or equal to 1,000,000 then it should be abbreviated with an M', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(4000000, metadata);
            expect(toReturn).toBe("4M");
        });
    });
    
    describe('FormatBucketValue given Float value', function () {
        var toReturn;

        var metadata = {
            DataType: "Float",
            FundamentalType: null
        };
        
        it('If value is less than 1 it should get rounded to 2 decimals places', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(0.025, metadata);
            expect(toReturn).toBe("0.03");
        });
        
        it('If value is greater than 1 it should only be rounded t o1 decimal place', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(1.61, metadata);
            expect(toReturn).toBe("1.6");
        });
        
        it('If value is less than 1000 then it should not be abbreviated', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(40, metadata);
            expect(toReturn).toBe(40);
        });
        
        it('If value is less than 1,000,000 then it should be abbreviated with a K', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(40000, metadata);
            expect(toReturn).toBe("40K");
        });
        
        it('If value is greater than or equal to 1,000,000 then it should be abbreviated with an M', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(4000000, metadata);
            expect(toReturn).toBe("4M");
        });
    });
    
    describe('FormatBucketValue given Short value', function () {
        var toReturn;

        var metadata = {
            DataType: "Short",
            FundamentalType: null
        };
        
        it('If value is less than 1 it should get rounded to 2 decimals places', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(0.025, metadata);
            expect(toReturn).toBe("0.03");
        });
        
        it('If value is greater than 1 it should only be rounded t o1 decimal place', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(1.61, metadata);
            expect(toReturn).toBe("1.6");
        });
        
        it('If value is less than 1000 then it should not be abbreviated', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(40, metadata);
            expect(toReturn).toBe(40);
        });
        
        it('If value is less than 1,000,000 then it should be abbreviated with a K', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(40000, metadata);
            expect(toReturn).toBe("40K");
        });
        
        it('If value is greater than or equal to 1,000,000 then it should be abbreviated with an M', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(4000000, metadata);
            expect(toReturn).toBe("4M");
        });
    });
    
    describe('FormatBucketValue given Long value', function () {
        var toReturn;

        var metadata = {
            DataType: "Long",
            FundamentalType: null
        };
        
        it('If value is less than 1 it should get rounded to 2 decimals places', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(0.025, metadata);
            expect(toReturn).toBe("0.03");
        });
        
        it('If value is greater than 1 it should only be rounded t o1 decimal place', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(1.61, metadata);
            expect(toReturn).toBe("1.6");
        });
        
        it('If value is less than 1000 then it should not be abbreviated', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(40, metadata);
            expect(toReturn).toBe(40);
        });
        
        it('If value is less than 1,000,000 then it should be abbreviated with a K', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(40000, metadata);
            expect(toReturn).toBe("40K");
        });
        
        it('If value is greater than or equal to 1,000,000 then it should be abbreviated with an M', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(4000000, metadata);
            expect(toReturn).toBe("4M");
        });
    });
    
    describe('FormatBucketValue given currency FundamentalType', function () {
        var toReturn;

        var metadata = {
            DataType: "Integer",
            FundamentalType: "currency"
        };
        
        
        it('If FundamentalType is currency and value is less than 1000 then it should not be abbreviated and have a currency symbol', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(40, metadata);
            expect(toReturn).toBe("$40");
        });
        
        it('If FundamentalType is currency and value is less than 1,000,000 then it should be abbreviated with a K and have a currency symbol', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(40000, metadata);
            expect(toReturn).toBe("$40K");
        });
        
        it('If FundamentalType is currency and value is greater than or equal to 1,000,000 then it should be abbreviated with an M and have a currency symbol', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(4000000, metadata);
            expect(toReturn).toBe("$4M");
        });
    });
    
    describe('FormatBucketValue given Year FundamentalType', function () {
        var toReturn;

        var metadata = {
            DataType: "Integer",
            FundamentalType: "Year"
        };
        
        
        it('should return 1995 if FundamentalType is year and value 1995', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(1995, metadata);
            expect(toReturn).toBe(1995);
        });
        
        it('should return 1993 if FundamentalType is year and value 1993', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue(1993, metadata);
            expect(toReturn).toBe(1993);
        });
    });
    
    describe('FormatBucketValue given Bit value', function () {
        var toReturn;

        var metadata = {
            DataType: "Bit",
            FundamentalType: null
        };
        
        
        it('If value is 0, return the string No', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue("0", metadata);
            expect(toReturn).toBe("No");
        });
        
        it('If value is false, return the string No', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue("false", metadata);
            expect(toReturn).toBe("No");
        });
        
        it('If value is 1, return the string Yes', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue("1", metadata);
            expect(toReturn).toBe("Yes");
        });
        
        it('If value is true, return the string Yes', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue("true", metadata);
            expect(toReturn).toBe("Yes");
        });
        
        it('If value is True, return the string Yes', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue("True", metadata);
            expect(toReturn).toBe("Yes");
        });
    });
    
    describe('FormatBucketValue given Boolean value', function () {
        var toReturn;

        var metadata = {
            DataType: "Boolean",
            FundamentalType: null
        };
        
        
        it('If value is 0, return the string No', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue("0", metadata);
            expect(toReturn).toBe("No");
        });
        
        it('If value is false, return the string No', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue("false", metadata);
            expect(toReturn).toBe("No");
        });
        
        it('If value is 1, return the string Yes', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue("1", metadata);
            expect(toReturn).toBe("Yes");
        });
        
        it('If value is true, return the string Yes', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue("true", metadata);
            expect(toReturn).toBe("Yes");
        });
        
        it('If value is True, return the string Yes', function () {
            toReturn = analyticAttributeUtility.FormatBucketValue("True", metadata);
            expect(toReturn).toBe("Yes");
        });
    });
    
    describe('GetAttributeBucketName given Boolean value', function () {
        var toReturn;

        var bucket1 = {
                ColumnName: "Attr1Name3",
                Values: [
                    "0"
                ],
                Lift: 10
            };
        
        var bucket2 = {
                ColumnName: "Attr1Name3",
                Values: [
                    "0.0"
                ],
                Lift: 10
            };
        
        var bucket3 = {
                ColumnName: "Attr1Name3",
                Values: [
                    "1"
                ],
                Lift: 10
            };
        
        var bucket4 = {
                ColumnName: "Attr1Name3",
                Values: [
                    "1.0"
                ],
                Lift: 10
            };
        
        var bucket5 = {
                ColumnName: "Attr1Name3",
                Values: [
                    "T"
                ],
                Lift: 10
            };
        
        var bucket6 = {
                ColumnName: "Attr1Name3",
                Values: [
                    "F"
                ],
                Lift: 10
            };
        
        var bucket7 = {
                ColumnName: "Attr1Name3",
                Values: [
                    "true"
                ],
                Lift: 10
            };
        
        var bucket8 = {
                ColumnName: "Attr1Name3",
                Values: [
                    "false"
                ],
                Lift: 10
            };
        
        var bucket9 = {
                ColumnName: "Attr1Name3",
                Values: [
                    "Yes"
                ],
                Lift: 10
            };
        
        var bucket10 = {
                ColumnName: "Attr1Name3",
                Values: [
                    "No"
                ],
                Lift: 10
            };
        
        var bucket11 = {
                ColumnName: "Attr1Name3",
                Values: [
                    "Y"
                ],
                Lift: 10
            };
        
        var bucket12 = {
                ColumnName: "Attr1Name3",
                Values: [
                    "N"
                ],
                Lift: 10
            };
        
        var bucket13 = {
                ColumnName: "Attr1Name3",
                Values: [
                    "0.00"
                ],
                Lift: 10
            };
        
        var bucket14 = {
                ColumnName: "Attr1Name3",
                Values: [
                    "1.00"
                ],
                Lift: 10
            };
        
        var bucket15 = {
                ColumnName: "Attr1Name3",
                Values: [
                    "-1.00"
                ],
                Lift: 10
            };
        
        var metadata = {
            DataType: null,
            FundamentalType: "boolean"
        };
        
        
        it('If value is 0, return the string No', function () {
            toReturn = analyticAttributeUtility.GetAttributeBucketName(bucket1, metadata);
            expect(toReturn).toBe("No");
        });
        
        it('If value is 0.0, return the string No', function () {
            toReturn = analyticAttributeUtility.GetAttributeBucketName(bucket2, metadata);
            expect(toReturn).toBe("No");
        });
        
        it('If value is 1, return the string Yes', function () {
            toReturn = analyticAttributeUtility.GetAttributeBucketName(bucket3, metadata);
            expect(toReturn).toBe("Yes");
        });
        
        it('If value is 1.0, return the string Yes', function () {
            toReturn = analyticAttributeUtility.GetAttributeBucketName(bucket4, metadata);
            expect(toReturn).toBe("Yes");
        });
        
        it('If value is T, return the string Yes', function () {
            toReturn = analyticAttributeUtility.GetAttributeBucketName(bucket5, metadata);
            expect(toReturn).toBe("Yes");
        });
        
        it('If value is F, return the string Yes', function () {
            toReturn = analyticAttributeUtility.GetAttributeBucketName(bucket6, metadata);
            expect(toReturn).toBe("No");
        });
        
        it('If value is true, return the string Yes', function () {
            toReturn = analyticAttributeUtility.GetAttributeBucketName(bucket7, metadata);
            expect(toReturn).toBe("Yes");
        });
        
        it('If value is false, return the string Yes', function () {
            toReturn = analyticAttributeUtility.GetAttributeBucketName(bucket8, metadata);
            expect(toReturn).toBe("No");
        });
        
        it('If value is Yes, return the string Yes', function () {
            toReturn = analyticAttributeUtility.GetAttributeBucketName(bucket9, metadata);
            expect(toReturn).toBe("Yes");
        });
        
        it('If value is No, return the string Yes', function () {
            toReturn = analyticAttributeUtility.GetAttributeBucketName(bucket10, metadata);
            expect(toReturn).toBe("No");
        });
        
        it('If value is Y, return the string Yes', function () {
            toReturn = analyticAttributeUtility.GetAttributeBucketName(bucket11, metadata);
            expect(toReturn).toBe("Yes");
        });
        
        it('If value is N, return the string Yes', function () {
            toReturn = analyticAttributeUtility.GetAttributeBucketName(bucket12, metadata);
            expect(toReturn).toBe("No");
        });
        
        it('If value is 0.00, return the string Yes', function () {
            toReturn = analyticAttributeUtility.GetAttributeBucketName(bucket13, metadata);
            expect(toReturn).toBe("No");
        });
        
        it('If value is 1.00, return the string Yes', function () {
            toReturn = analyticAttributeUtility.GetAttributeBucketName(bucket14, metadata);
            expect(toReturn).toBe("Yes");
        });
        
        it('If value is -1.00, return the string Yes', function () {
            toReturn = analyticAttributeUtility.GetAttributeBucketName(bucket15, metadata);
            expect(toReturn).toBe("Not Available");
        });
        
    });
    
    describe('ShouldShowNullBucket given the null bucket', function () {
        var toReturn;

        var nullContinuousBucket = {
            ColumnName: "Attr1Name3",
            Values: [
                null
            ],
            LowerInclusive: null,
            UpperExclusive: null,
            Lift: 10
        };
        
        var nullDiscreteBucket = {
            ColumnName: "Attr1Name3",
            Values: [
                null
            ],
            LowerInclusive: null,
            UpperExclusive: null,
            Lift: 10
        };
        
        it('(Continuous) The Lift is below the threshold so it should return false.', function () {
            toReturn = analyticAttributeUtility.ShouldShowNullBucket(nullContinuousBucket, 100);
            expect(toReturn).toBe(false);
        });
        
        it('(Continuous) The Lift is above the threshold so it should return true.', function () {
            toReturn = analyticAttributeUtility.ShouldShowNullBucket(nullContinuousBucket, 5);
            expect(toReturn).toBe(true);
        });
        
        it('(Discrete) The Lift is below the threshold so it should return false.', function () {
            toReturn = analyticAttributeUtility.ShouldShowNullBucket(nullDiscreteBucket, 100);
            expect(toReturn).toBe(false);
        });
        
        it('(Discrete) The Lift is above the threshold so it should return true.', function () {
            toReturn = analyticAttributeUtility.ShouldShowNullBucket(nullDiscreteBucket, 5);
            expect(toReturn).toBe(true);
        });
    });
    
    describe('ShouldShowNullBucket discrete not null bucket', function () {
        var toReturn;

        var bucket = {
            ColumnName: "Attr1Name3",
            Values: [
                "test"
            ],
            LowerInclusive: null,
            UpperExclusive: null,
            Lift: 10
        };
        
        it('The bucket is not the null bucket so it should return true.', function () {
            toReturn = analyticAttributeUtility.ShouldShowNullBucket(bucket, 5);
            expect(toReturn).toBe(true);
        });
    });
    
    describe('ShouldShowNullBucket continuous not null bucket', function () {
        var toReturn;

        var bucket = {
            ColumnName: "Attr1Name3",
            Values: null,
            LowerInclusive: 10,
            UpperExclusive: 50,
            Lift: 10
        };
        
        it('The bucket is not the null bucket so it should return true.', function () {
            toReturn = analyticAttributeUtility.ShouldShowNullBucket(bucket, 5);
            expect(toReturn).toBe(true);
        });
    });
    
    describe('FormatLift tests', function () {
        var toReturn;
        
        it('null should return null', function () {
            toReturn = analyticAttributeUtility.FormatLift(null);
            expect(toReturn).toBe(null);
        });
        
        it('n should return null', function () {
            toReturn = analyticAttributeUtility.FormatLift("n");
            expect(toReturn).toBe(null);
        });
        
        it('0 should round to 0', function () {
            toReturn = analyticAttributeUtility.FormatLift(0);
            expect(toReturn).toBe("0");
        });
        
        it('0.99 should round to 1.0', function () {
            toReturn = analyticAttributeUtility.FormatLift(0.99);
            expect(toReturn).toBe("1.0");
        });
        
        it('0.1122312 should round to 0.1', function () {
            toReturn = analyticAttributeUtility.FormatLift(0.1122312);
            expect(toReturn).toBe("0.1");
        });
    });
    
    describe('ShowBasedOnTags given null parameters', function () {
        var toReturn;
        
        var attributeMetadata = {
            Tags: ["Internal"]
        };
        var widgetConfig = {
            RequiredTags: ["Internal"],
            ExcludedTags: []
        };
        
        it('Null widgetConfig should return false', function () {
            toReturn = analyticAttributeUtility.ShowBasedOnTags(null, attributeMetadata);
            expect(toReturn).toBe(false);
        });
        
        it('Null attributeMetadata should return false', function () {
            toReturn = analyticAttributeUtility.ShowBasedOnTags(widgetConfig, null);
            expect(toReturn).toBe(false);
        });
        
        it('Null attributeMetadata and  null widgetConfig should return false', function () {
            toReturn = analyticAttributeUtility.ShowBasedOnTags(null, null);
            expect(toReturn).toBe(false);
        });
        
        attributeMetadata.Tags = null;
        it('Null attributeMetadata.Tags should return true', function () {
            toReturn = analyticAttributeUtility.ShowBasedOnTags(widgetConfig, attributeMetadata);
            expect(toReturn).toBe(true);
        });
        
        attributeMetadata.Tags = ["Internal"];
        widgetConfig.RequiredTags = null;
        it('Null widgetConfig.RequiredTags should return true', function () {
            toReturn = analyticAttributeUtility.ShowBasedOnTags(widgetConfig, attributeMetadata);
            expect(toReturn).toBe(true);
        });
        
        attributeMetadata.Tags = ["Internal"];
        widgetConfig.RequiredTags = ["Internal"];
        widgetConfig.ExcludedTags = null;
        it('Null widgetConfig.ExcludedTags should return true', function () {
            toReturn = analyticAttributeUtility.ShowBasedOnTags(widgetConfig, attributeMetadata);
            expect(toReturn).toBe(true);
        });
    });
    
    describe('ShowBasedOnTags given required tags', function () {
        var toReturn;
        
        var attributeMetadata = {
            Tags: ["Internal"]
        };
        var widgetConfig = {
            RequiredTags: ["Internal"],
            ExcludedTags: []
        };
        
        it('should return true if attributeMetadata only has 1 Tag and it is required', function () {
            toReturn = analyticAttributeUtility.ShowBasedOnTags(widgetConfig, attributeMetadata);
            expect(toReturn).toBe(true);
        });
        
        var attributeMetadata1 = {
            Tags: ["Test", "Internal"]
        };
        it('should return true if attributeMetadata has 2 Tags and 1 is required', function () {
            toReturn = analyticAttributeUtility.ShowBasedOnTags(widgetConfig, attributeMetadata2);
            expect(toReturn).toBe(true);
        });
        
        var widgetConfig2 = {
            RequiredTags: ["Internal", "Test"],
            ExcludedTags: []
        };
        var attributeMetadata2 = {
            Tags: ["Test", "Internal", "Garbage"]
        };
        it('should return true if attributeMetadata has both required Tags', function () {
            toReturn = analyticAttributeUtility.ShowBasedOnTags(widgetConfig2, attributeMetadata2);
            expect(toReturn).toBe(true);
        });
        
        var widgetConfig3 = {
            RequiredTags: ["Internal", "Test"],
            ExcludedTags: []
        };
        var attributeMetadata3 = {
            Tags: ["Garbage", "Internal"]
        };
        it('should return false if attributeMetadata has 1 required Tag, but is missing one', function () {
            toReturn = analyticAttributeUtility.ShowBasedOnTags(widgetConfig3, attributeMetadata3);
            expect(toReturn).toBe(false);
        });
        
        var widgetConfig4 = {
            RequiredTags: [],
            ExcludedTags: []
        };
        it('should return true if widgetConfig has no required tags', function () {
            toReturn = analyticAttributeUtility.ShowBasedOnTags(widgetConfig4, attributeMetadata);
            expect(toReturn).toBe(true);
        });
    });
    
    describe('ShowBasedOnTags given excluded tags', function () {
        var toReturn;
        
        var attributeMetadata = {
            Tags: ["External"]
        };
        var widgetConfig = {
            RequiredTags: [],
            ExcludedTags: ["Internal"]
        };
        
        it('should return true if attributeMetadata only has 1 Tag and it is not excluded', function () {
            toReturn = analyticAttributeUtility.ShowBasedOnTags(widgetConfig, attributeMetadata);
            expect(toReturn).toBe(true);
        });
        
        var attributeMetadata2 = {
            Tags: ["Internal"]
        };
        it('should return false if attributeMetadata only has 1 Tag and it is excluded', function () {
            toReturn = analyticAttributeUtility.ShowBasedOnTags(widgetConfig, attributeMetadata2);
            expect(toReturn).toBe(false);
        });
        
        var attributeMetadata3 = {
            Tags: ["Test", "Internal"]
        };
        it('should return false if attributeMetadata has 2 Tags and 1 is excluded', function () {
            toReturn = analyticAttributeUtility.ShowBasedOnTags(widgetConfig, attributeMetadata3);
            expect(toReturn).toBe(false);
        });
        
        var widgetConfig2 = {
            RequiredTags: [],
            ExcludedTags: ["Internal"]
        };
        it('should return true if widgetConfig has no Excluded Tags', function () {
            toReturn = analyticAttributeUtility.ShowBasedOnTags(widgetConfig2, attributeMetadata);
            expect(toReturn).toBe(true);
        });
    });
    
    describe('ShowBasedOnTags given required and excluded tags', function () {
        var toReturn;
        
        var attributeMetadata = {
            Tags: ["External"]
        };
        var widgetConfig = {
            RequiredTags: ["External"],
            ExcludedTags: ["Internal"]
        };
        
        it('should return true if widgetConfig has 1 Required Tag and 1 Excluded Tag while attributeMetadata has the Required Tag and does not have the Excluded Tag.', function () {
            toReturn = analyticAttributeUtility.ShowBasedOnTags(widgetConfig, attributeMetadata);
            expect(toReturn).toBe(true);
        });
        
        var attributeMetadata2 = {
            Tags: ["External", "Internal"]
        };
        it('should return false if widgetConfig has 1 Required Tag and 1 Excluded Tag while attributeMetadata has the Required Tag, but has the Excluded Tag.', function () {
            toReturn = analyticAttributeUtility.ShowBasedOnTags(widgetConfig, attributeMetadata2);
            expect(toReturn).toBe(false);
        });
        
        var attributeMetadata3 = {
            Tags: ["Test"]
        };
        it('should return false if widgetConfig has 1 Required Tag and 1 Excluded Tag while attributeMetadata does not have the Required Tag and does not have the Excluded Tag.', function () {
            toReturn = analyticAttributeUtility.ShowBasedOnTags(widgetConfig, attributeMetadata3);
            expect(toReturn).toBe(false);
        });
        
        var attributeMetadata4 = {
            Tags: ["Test", "External", "Internal"]
        };
        var widgetConfig4 = {
            RequiredTags: ["Test", "External"],
            ExcludedTags: ["Internal"]
        };
        it('should return false if widgetConfig has 2 Required Tags and 1 Excluded Tag while attributeMetadata has both Required Tag, but has the Excluded Tag.', function () {
            toReturn = analyticAttributeUtility.ShowBasedOnTags(widgetConfig4, attributeMetadata4);
            expect(toReturn).toBe(false);
        });
    });
    
    describe('IsAllowedForInsights tests', function () {
        var toReturn;
        
        var sampleMetadata = {
            ColumnName: "FirstPredictor",
            DisplayName: "One",
            DataType: "Number",
            ApprovedUsage: null
        };
        
        it('should return true if ApprovedUsage is null', function () {
            toReturn = analyticAttributeUtility.IsAllowedForInsights(sampleMetadata);
            expect(toReturn).toBe(true);
        });
        
        var sampleMetadata2 = {
            ColumnName: "FirstPredictor",
            DisplayName: "One",
            DataType: "Number",
            ApprovedUsage: []
        };
        it('should return true if ApprovedUsage is an empty array', function () {
            toReturn = analyticAttributeUtility.IsAllowedForInsights(sampleMetadata2);
            expect(toReturn).toBe(true);
        });
        
        var sampleMetadata3 = {
            ColumnName: "FirstPredictor",
            DisplayName: "One",
            DataType: "Number",
            ApprovedUsage: ["ModelAndAllInsights"]
        };
        it('should return true if ApprovedUsage contains ModelAndAllInsights as a value', function () {
            toReturn = analyticAttributeUtility.IsAllowedForInsights(sampleMetadata3);
            expect(toReturn).toBe(true);
        });
        
        var sampleMetadata4 = {
            ColumnName: "FirstPredictor",
            DisplayName: "One",
            DataType: "Number",
            ApprovedUsage: ["None"]
        };
        it('should return false if ApprovedUsage does not contain ModelAndAllInsights as a value', function () {
            toReturn = analyticAttributeUtility.IsAllowedForInsights(sampleMetadata4);
            expect(toReturn).toBe(false);
        });
    });
    
    describe('FormatBooleanValueForDisplay tests', function () {
        var toReturn;
        
        it('should return an empty string if given no parameters', function () {
            toReturn = analyticAttributeUtility.FormatBooleanValueForDisplay();
            expect(toReturn).toBe("");
        });
        
        it('should return an empty string if given null as a value', function () {
            toReturn = analyticAttributeUtility.FormatBooleanValueForDisplay(null);
            expect(toReturn).toBe("");
        });
        
        it('should return an empty string if given empty string as a value', function () {
            toReturn = analyticAttributeUtility.FormatBooleanValueForDisplay("");
            expect(toReturn).toBe("");
        });
        
        it('should return an empty string if given a non-truthy string as a value', function () {
            toReturn = analyticAttributeUtility.FormatBooleanValueForDisplay("garbage");
            expect(toReturn).toBe("");
        });
        
        it('should return Yes if given the boolean value of true', function () {
            toReturn = analyticAttributeUtility.FormatBooleanValueForDisplay(true);
            expect(toReturn).toBe("Yes");
        });
        
        it('should return No if given the boolean value of false', function () {
            toReturn = analyticAttributeUtility.FormatBooleanValueForDisplay(false);
            expect(toReturn).toBe("No");
        });
        
        it('should return No if given the string value of false', function () {
            toReturn = analyticAttributeUtility.FormatBooleanValueForDisplay("false");
            expect(toReturn).toBe("No");
        });
        
        it('should return Yes if given the string value of true', function () {
            toReturn = analyticAttributeUtility.FormatBooleanValueForDisplay("true");
            expect(toReturn).toBe("Yes");
        });
        
        it('should return Yes if given the integer value of 1', function () {
            toReturn = analyticAttributeUtility.FormatBooleanValueForDisplay(1);
            expect(toReturn).toBe("Yes");
        });
        
        it('should return No if given the integer value of 0', function () {
            toReturn = analyticAttributeUtility.FormatBooleanValueForDisplay(0);
            expect(toReturn).toBe("No");
        });
        
        it('should return No if given the string value of 0', function () {
            toReturn = analyticAttributeUtility.FormatBooleanValueForDisplay("0");
            expect(toReturn).toBe("No");
        });
        
        it('should return Yes if given the string value of 1', function () {
            toReturn = analyticAttributeUtility.FormatBooleanValueForDisplay("1");
            expect(toReturn).toBe("Yes");
        });
    });
});