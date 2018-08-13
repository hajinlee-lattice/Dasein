'use strict';

var Siderbar = function() {
    var nav = element(by.id('nav'));
    this.EnrichmentLink = nav.element(by.css('[ui-sref="home.enrichment"]'));
    this.ManageUsersLink = nav.element(by.css('[ui-sref="home.users"]'));
    this.JobsLink = nav.element(by.css('[ui-sref="home.jobs.status"]'));
    this.MarketoSettingsLink = nav.element(by.css('[ui-sref="home.marketosettings.apikey"]'));

    this.AttributesLink = nav.element(by.css('[ui-sref="home.model.attributes"]')); 
    this.PerformanceLink = nav.element(by.css('[ui-sref="home.model.performance"]')); 
    this.SampleLeadsLink = nav.element(by.css('[ui-sref="home.model.leads"]')); 
    this.ModelSummaryLink = nav.element(by.css('[ui-sref="home.model.summary"]')); 
    this.ScoringLink = nav.element(by.css('[ui-sref="home.model.jobs"]')); 
    this.RefineAndCloneLink = nav.element(by.css('[ui-sref="home.model.refine"]')); 

    this.PredictionModelsLink = nav.element(by.linkText('Prediction Models'));
};

module.exports = new Siderbar();