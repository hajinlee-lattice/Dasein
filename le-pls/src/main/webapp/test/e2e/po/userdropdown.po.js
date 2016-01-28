'use strict';

var UserDropdown = function() {
    this.signout = element(by.linkText('Sign Out'));
    this.ManageUsersLink = element(by.linkText('Manage Users'));
    this.ActivateModelLink = element(by.linkText('Activate Model'));
    this.SystemSetupLink = element(by.linkText('System Setup'));
    this.ModelCreationHistoryLink = element(by.linkText('Model Creation History'));
    this.updatePassword = element(by.linkText('Update Password'));
    this.modelCreationHistory = element(by.linkText('Model Creation History'));
    this.SetupLink = element(by.linkText('Manage Fields'));
    this.TenantDeploymentWizard = element(by.linkText('Deployment Wizard'));
    this.LeadEnrichment = element(by.linkText('Lead Enrichment'));

    this.getUserLink = function(name) {
        return element(by.linkText(name));
    };

    this.toggleDropdown = function() {
        element(by.className('nav-personal')).click();
        browser.waitForAngular();
    };
};

module.exports = new UserDropdown();