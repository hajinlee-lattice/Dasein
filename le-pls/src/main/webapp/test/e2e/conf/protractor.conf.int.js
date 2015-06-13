'use strict';

var common = require('./protractor.conf.common');

var config = common.generateConfig('..');

config.params.tenantId = "IntegrationTestPLSTenant2.IntegrationTestPLSTenant2.Production";
config.params.alternativeTenantId = "IntegrationTestPLSTenant1.IntegrationTestPLSTenant1.Production";

exports.config = config;