'use strict';

var common = require('./protractor.conf.common');

var config = common.generateConfig('..');

config.params.tenantId = "IntegrationTestPLSContract.Tenant2.Production";
config.params.alternativeTenantId = "IntegrationTestPLSContract.Tenant1.Production";

exports.config = config;