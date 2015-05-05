'use strict';

var common = require('./protractor.conf.common');

var config = common.generateConfig('..');

config.params.tenantId = "Tenant2";
config.params.alternativeTenantId = "Tenant1";

exports.config = config;