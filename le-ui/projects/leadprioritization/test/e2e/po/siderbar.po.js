'use strict';

var Siderbar = function() {
    var nav = element(by.id('nav'));
    this.PredictionModelsLink = nav.element(by.linkText('Prediction Models'));
    this.CreateModelLink = nav.element(by.linkText('Create a Model'));
    this.ManageUsersLink = nav.element(by.linkText('Manage Users'));
    this.ModelCreationHistoryLink = nav.element(by.linkText('Model Creation History'));
    this.JobsLink = nav.element(by.linkText('Jobs'));
    this.MarketoSettingsLink = nav.element(by.linkText('Marketo Settings'));
    this.AttributesLink = nav.element(by.linkText('Attributes'));
    this.PerformanceLink = nav.element(by.linkText('Performance'));
    this.SampleLeadsLink = nav.element(by.linkText('Sample Leads'));
    this.ModelSummaryLink = nav.element(by.linkText('Model Summary'));
    this.ScoringLink = nav.element(by.linkText('Scoring'));
    this.RefineAndCloneLink = nav.element(by.linkText('Refine & Clone'));
};

module.exports = new Siderbar();