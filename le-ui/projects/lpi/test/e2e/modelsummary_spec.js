'use strict';

describe('model summary', function() {
    var loginPage = require('./po/login.po');
    var siderbar = require('./po/siderbar.po');
    var modelList = require('./po/modellist.po');
    var helper = require('./po/helper.po');

    var modelNamesOnHDFS = "PLSModel-Eloqua";
    var modelNamesNotOnHDFS = "PLSModel-Eloqua-00";
    var modelOnHDFSAndPage;

    var expectedAttributesPresentOnInternalAdminPage = ["Model Health Score", "Tenant ID", "Tenant Name",
        "Internal ID", "Model Details", "Scored Leads", "Model Performance"];

    var modelDetailFiles = ["modelsummary.json", "predictors.csv", "diagnostics.json", "metadata.avsc"];
    var scoredLeadFiles = ["readout.csv", "scores.csv"];
    var modelPerformanceFiles = ["performance.csv", "rfmodel.csv"];

    it('should have at least one model with information on HDFS', function() {
        loginPage.loginAsSuperAdmin();
        var modelNamesElement = element.all(by.css('.model-title h2.editable'));
        modelNamesElement.getText().then(function(modelNamesOnPage) {
            for (var i = 0; i < modelNamesOnPage.length; i++) {
                var modelNameOnPage = modelNamesOnPage[i];
                if (modelNameOnPage.indexOf(modelNamesOnHDFS) >= 0 &&
                    modelNameOnPage.indexOf(modelNamesNotOnHDFS) < 0) {
                    modelOnHDFSAndPage = modelNameOnPage;
                    break;
                }
            }
            expect(modelOnHDFSAndPage != null).toBe(true);
        });
    });

    it('should have all the necessary attributes on the table', function() {
        element(by.cssContainingText('.editable', modelOnHDFSAndPage)).click();
        browser.driver.sleep(1000);
        siderbar.ModelSummaryLink.click();
        browser.driver.sleep(1000);

        var headers = element.all(by.css('table.table tbody tr th')).map(function(tableHeader) {
            return tableHeader.getText();
        });
        expect(headers).toEqual(expectedAttributesPresentOnInternalAdminPage);
    });

    it('all the links should be downloadable', function() {
        var modelDetails = element(by.cssContainingText('td', 'Model Summary JSON'));
        var scoredLeads = element(by.cssContainingText('td', 'Readout Sample CSV'));
        var modelPerformance = element(by.cssContainingText('td', 'Performance Chart CSV'));

        var modelDetailDownloadLinks = modelDetails.all(by.linkText('Download'));
        var scoredLeadDownloadLinks = scoredLeads.all(by.linkText('Download'));
        var modelPerformanceDownloadLinks = modelPerformance.all(by.linkText('Download'));

        expect(modelDetailDownloadLinks.count()).toBe(modelDetailFiles.length);
        expect(scoredLeadDownloadLinks.count()).toBe(scoredLeadFiles.length);
        expect(modelPerformanceDownloadLinks.count()).toBe(modelPerformanceFiles.length);

        removeAllDownloadedFiles();

        // Download all files here
        for (var i = 0; i < modelDetailFiles.length; i++) {
            modelDetailDownloadLinks.get(i).click();
            browser.driver.sleep(5000);
        }
        for (var i = 0; i < scoredLeadFiles.length; i++) {
            scoredLeadDownloadLinks.get(i).click();
        }
        for (var i = 0; i < modelPerformanceFiles.length; i++) {
            modelPerformanceDownloadLinks.get(i).click();
        }

        checkAllFilesAreDownloaded();
    });

    function removeAllDownloadedFiles() {
        for (var i = 0; i < modelDetailFiles.length; i++) {
            helper.removeFile(modelDetailFiles[i]);
        }

        for (var i = 0; i < scoredLeadFiles.length; i++) {
            helper.removeFile(scoredLeadFiles[i]);
        }

        for (var i = 0; i < modelPerformanceFiles.length; i++) {
            helper.removeFile(modelPerformanceFiles[i]);
        }
    }

    function checkAllFilesAreDownloaded() {
        for (var i = 0; i < modelDetailFiles.length; i++) {
            helper.fileExists(modelDetailFiles[i]);
        }

        for (var i = 0; i < scoredLeadFiles.length; i++) {
            helper.fileExists(scoredLeadFiles[i]);
        }

        for (var i = 0; i < modelPerformanceFiles.length; i++) {
            helper.fileExists(modelPerformanceFiles[i]);
        }
    }
});

