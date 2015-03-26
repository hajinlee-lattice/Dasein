'use strict';

var Login = function() {
	this.email = element(by.model('username'));
	this.password = element(by.model('password'));
    this.loginButton = element(by.id('loginButton'));
 
	this.get = function() {
	    var width = 1100;
        var height = 768;
        browser.driver.manage().window().setSize(width, height);
        browser.get('/');
	};

    this.loginUser = function(name, password) {
        this.get();
        this.email.sendKeys(name);
        this.password.sendKeys(password);     
        this.loginButton.click();
        browser.waitForAngular();
    };

  	this.loginAsAdmin = function() {
  		this.loginUser(browser.params.adminUsername, browser.params.adminPassword);
    };

  	this.loginAsNonAdmin = function() {
  		this.loginUser(browser.params.nonAdminUsername, browser.params.nonAdminPassword);
    };

};

module.exports = new Login();