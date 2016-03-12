angular.module('test.testData.ManageFieldsTestDataService', [])
.service('ManageFieldsTestDataService', function() {

    this.GetSampleFields = function () {
        return [
            {
                "ColumnName": "ID",
                "Source": "Marketo",
                "SourceToDisplay": "Marketo",
                "Object": "Lead",
                "Category": "Lead Information",
                "DisplayName": "ID",
                "ApprovedUsage": "None",
                "Tags": "Internal",
                "FundamentalType": "URI",
                "Description": null,
                "DisplayDiscretization": null,
                "StatisticalType": "ratio"
            },
            {
                "ColumnName": "Email",
                "Source": "Marketo",
                "SourceToDisplay": "Marketo",
                "Object": "Lead",
                "Category": "Lead Information",
                "DisplayName": "Email Address",
                "ApprovedUsage": "Model",
                "Tags": "Internal",
                "FundamentalType": "URI",
                "Description": null,
                "DisplayDiscretization": null,
                "StatisticalType": "ratio"
            },
            {
                "ColumnName": "Phone",
                "Source": "Marketo",
                "SourceToDisplay": "Marketo",
                "Object": "Account",
                "Category": "Marketing Activity",
                "DisplayName": "Phone Number",
                "ApprovedUsage": "Model",
                "Tags": "Internal",
                "FundamentalType": "URI",
                "Description": null,
                "DisplayDiscretization": null,
                "StatisticalType": "ratio"
            },
            {
                "ColumnName": "Address",
                "Source": "Salesforce",
                "SourceToDisplay": "Salesforce",
                "Object": "Lead",
                "Category": "Marketing Activity",
                "DisplayName": "Address",
                "ApprovedUsage": "",
                "Tags": "Internal",
                "FundamentalType": "URI",
                "Description": null,
                "DisplayDiscretization": null,
                "StatisticalType": "ratio"
            },
            {
                "ColumnName": "Employees",
                "Source": "PD_Alexa_Source_Import",
                "SourceToDisplay": "Lattice Data Cloud",
                "Object": null,
                "Category": "Lead Information",
                "DisplayName": "Address",
                "ApprovedUsage": "",
                "Tags": "",
                "FundamentalType": null,
                "Description": null,
                "DisplayDiscretization": null,
                "StatisticalType": null
            }
        ];
    }; //end GetSampleFields()
});