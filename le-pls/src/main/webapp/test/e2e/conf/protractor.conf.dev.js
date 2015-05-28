'use strict';

var common = require('./protractor.conf.common');

var config = common.generateConfig('..');

config.params.tenantId = "DevelopTestPLSContract.DevelopTestTenant2.Production";
config.params.alternativeTenantId = "DevelopTestPLSContract.DevelopTestTenant1.Production";

exports.config = config;