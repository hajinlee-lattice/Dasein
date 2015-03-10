'use strict';

var Login = function() {
	this.email = element(by.model('username'));
	this.password = element(by.model('password'));
    this.loginButton = element(by.id('loginButton'));
 
	this.get = function() {
	    var width = 1024;
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
        browser.driver.sleep(2000);
    }

  	this.loginAsAdmin = function() {
  		this.loginUser('bnguyen@lattice-engines.com', 'tahoe');
    }

  	this.loginAsNonAdmin = function() {
  		this.loginUser('lming@lattice-engines.com', 'admin');
    }

};

module.exports = new Login();