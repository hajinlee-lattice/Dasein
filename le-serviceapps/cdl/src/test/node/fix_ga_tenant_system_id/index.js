'use strict';

const cmd = require('commander');
const Sequelize = require('sequelize');
const fs = require('fs');

const DATABASE = 'PLS_MultiTenant';

/*
 * steps:
 * 1. generate random IDs (to be used in this script & jupyter script)
 * 2. show data collection tables (make sure this is an old ga tenant)
 * 3. fix data in account/contact batch store & system store (jupyter script)
 * 4. fix metadata (this script)
 */

/**
 * Generate random account|contact system ID with the same format as atlas
 */
cmd.command('generate-id')
    .description('generate random system account/contact ids')
    .action((env, option) => {
        console.log(`Generated account system id = ${newSystemId('AccountId')}`);
        console.log(`Generated contact system id = ${newSystemId('ContactId')}`);
    });

/**
 * Show default system and related template/table info
 */
cmd.command('show-tables <tenant>')
    .description('show batch store tables that needs fixing')
    .requiredOption('-C, --config <configPath>', 'path to json config file')
    .action(async (tenantId, option) => {
        console.log(`Config path = ${option.config}, Tenant = ${tenantId}`)
        let conf = JSON.parse(fs.readFileSync(option.config).toString());
        console.log(`Config = ${JSON.stringify(conf, null, 2)}`);
        const client = getClient(conf);

        try {
            let system = await getDefaultSystem(client, tenantId);
            console.log(`1. Default system exist. PID=${system.PID}, accSysId=${system.ACCOUNT_SYSTEM_ID}, contactSysId=${system.CONTACT_SYSTEM_ID}, mapToAccount=${!!system.MAP_TO_LATTICE_ACCOUNT[0]}, mapToContact=${!!system.MAP_TO_LATTICE_CONTACT[0]}`);
            let dataCollection = await getDataCollection(client, tenantId);
            if (dataCollection) {
                console.log(`Data collection for tenant ${tenantId} = ${JSON.stringify(dataCollection, null, 2)}`);
                let tables = await getDataCollectionTables(client, dataCollection.PID, dataCollection.VERSION);
                let extracts = Object.keys(tables).length === 0 ? {} : await getExtracts(client, Object.values(tables));
                let paths = {};
                for (let key in tables) {
                    paths[key] = extracts[tables[key]];
                }
                console.log(`Tables = ${JSON.stringify(tables, null, 2)}`);
                console.log(`Paths = ${JSON.stringify(paths, null, 2)}`);

                if ('ConsolidatedAccount' in tables) {
                    let accIdAttrs = await getAttributes(client, tables.ConsolidatedAccount, 'CustomerAccountId');
                    console.log(`IDs in account batch store = ${attrsToDebugStr(accIdAttrs)}`);
                }
                if ('SystemAccount' in tables) {
                    let accIdAttrs = await getAttributes(client, tables.SystemAccount, 'CustomerAccountId');
                    console.log(`IDs in system account store = ${attrsToDebugStr(accIdAttrs)}`);
                }

                if ('ConsolidatedContact' in tables) {
                    let accIdAttrs = await getAttributes(client, tables.ConsolidatedContact, 'CustomerAccountId');
                    let contactIdAttrs = await getAttributes(client, tables.ConsolidatedContact, 'CustomerContactId');
                    console.log(`IDs in contact batch store Account=${attrsToDebugStr(accIdAttrs)}, Contact=${attrsToDebugStr(contactIdAttrs)}`);
                }
                if ('SystemContact' in tables) {
                    let accIdAttrs = await getAttributes(client, tables.SystemContact, 'CustomerAccountId');
                    let contactIdAttrs = await getAttributes(client, tables.SystemContact, 'CustomerContactId');
                    console.log(`IDs in system contact store Account=${attrsToDebugStr(accIdAttrs)}, Contact=${attrsToDebugStr(contactIdAttrs)}`);
                }
            } else {
                console.log('No data collection found');
            }

            let accountTemplate = await getDefaultAccountTemplate(client, tenantId);
            let contactTemplate = await getDefaultContactTemplate(client, tenantId);
            if (accountTemplate) {
                console.log(`Default account template exist. PID=${accountTemplate.PID}, templateTableId=${accountTemplate.FK_TEMPLATE_ID}, feedId=${accountTemplate.FK_FEED_ID}`);
            } else {
                console.log(`No default account template found`);
            }
            if (contactTemplate) {
                console.log(`Default contact template exist. PID=${contactTemplate.PID}, templateTableId=${contactTemplate.FK_TEMPLATE_ID}, feedId=${contactTemplate.FK_FEED_ID}`);
            } else {
                console.log(`No default contact template found`);
            }
        } catch (e) {
            console.error(`Failed to show tables for tenant ${tenantId}`, e);
        } finally {
            client.close();
        }
    });

/**
 * Fix metadata in MySQL for GA tenants by
 * 1. copying Customer[Account|Contact]Id to attribute metadata to a specified ID.
 * 2. set specified IDs to default system
 * 3. set mapToLattice[Account|Contact] flag in default system to true
 *
 * Following tables will be fixed:
 * 1. account batch store table
 * 2. contact batch store table
 * 3. account system store table
 * 4. contact system store table
 * 5. default system's account import template
 * 6. default system's contact import template
 *
 * For system store table, all attribute with suffix Customer[Account|Contact]Id will be copied
 * to new attribute with suffix as the specified ID, these attribute means value from different
 * template
 *
 * There is a check that fails if any system ID or mapToLattice[Account|Contact] is set in default
 * system, meaning that this is not an old GA tenant
 */
cmd.command('fix-tables <tenant>')
    .description('fix system ids metadata (batch store tables, system, etc) in tenant')
    .requiredOption('-C, --config <configPath>', 'path to json config file')
    .requiredOption('-a, --account-id <accountId>', 'system account id')
    .requiredOption('-c, --contact-id <contactId>', 'system contact id')
    .option('-s, --skip', 'skip system validation, force reset system IDs even it is already set')
    .action(async (tenantId, option) => {
        console.log(`Config path = ${option.config}, Tenant = ${tenantId}, skipValidation=${option.skip}`)
        let conf = JSON.parse(fs.readFileSync(option.config).toString());
        console.log(`Config = ${JSON.stringify(conf, null, 2)}`);
        const client = getClient(conf);
        let Attribute = client.import('./models/METADATA_ATTRIBUTE');

        let accId = option.accountId;
        let contactId = option.contactId;
        try {
            let system = await getDefaultSystem(client, tenantId);
            console.log(`1. Default system exist. PID=${system.PID}, accSysId=${system.ACCOUNT_SYSTEM_ID}, contactSysId=${system.CONTACT_SYSTEM_ID}, mapToAccount=${!!system.MAP_TO_LATTICE_ACCOUNT[0]}, mapToContact=${!!system.MAP_TO_LATTICE_CONTACT[0]}`);
            if (!option.skip) {
                checkSystem(system);
            }

            // set default systems
            await setAccountSystemId(client, system.PID, accId);
            await setContactSystemId(client, system.PID, contactId);
            await mapToLatticeEntity(client, system.PID);

            // set default system templates
            let accountTemplate = await getDefaultAccountTemplate(client, tenantId);
            if (accountTemplate) {
                console.log(`3. Default account template exist. PID=${accountTemplate.PID}, templateTableId=${accountTemplate.FK_TEMPLATE_ID}, feedId=${accountTemplate.FK_FEED_ID}`);
                await addAccountId(client, accountTemplate, accId, false, Attribute);
            } else {
                console.log('3. Account template in default system does not exist');
            }

            let contactTemplate = await getDefaultContactTemplate(client, tenantId);
            if (contactTemplate) {
                console.log(`3. Default contact template exist. PID=${contactTemplate.PID}, templateTableId=${contactTemplate.FK_TEMPLATE_ID}, feedId=${contactTemplate.FK_FEED_ID}`);
                await addContactId(client, contactTemplate, contactId, false, Attribute);
                await addAccountId(client, contactTemplate, accId, false, Attribute);
            } else {
                console.log('4. Contact template in default system does not exist');
            }

            let dataCollection = await getDataCollection(client, tenantId);
            if (dataCollection) {
                console.log(`5. DataCollection found. PID=${dataCollection.PID}, Version=${dataCollection.VERSION}`);
                let tables = await getDataCollectionTables(client, dataCollection.PID, dataCollection.VERSION);
                // account batch store
                if ('ConsolidatedAccount' in tables) {
                    let tableId = tables['ConsolidatedAccount'];
                    console.log(`5-1. found account batch store. PID=${tableId}`);
                    await addAccountId(client, { FK_TEMPLATE_ID: tableId }, accId, true, Attribute);
                }
                // account system store
                if ('SystemAccount' in tables) {
                    let tableId = tables['SystemAccount'];
                    console.log(`5-2. found account system batch store. PID=${tableId}`);
                    await copyAttrs(client, tableId, 'CustomerAccountId', accId, Attribute);
                }
                // contact batch store
                if ('ConsolidatedContact' in tables) {
                    let tableId = tables['ConsolidatedContact'];
                    console.log(`5-3. found contact batch store. PID=${tableId}`);
                    await addAccountId(client, { FK_TEMPLATE_ID: tableId }, accId, true, Attribute);
                    await addContactId(client, { FK_TEMPLATE_ID: tableId }, contactId, true, Attribute);
                }
                // contact system store
                if ('SystemContact' in tables) {
                    let tableId = tables['SystemContact'];
                    console.log(`5-4. found contact system batch store. PID=${tableId}`);
                    await copyAttrs(client, tableId, 'CustomerAccountId', accId, Attribute);
                    await copyAttrs(client, tableId, 'CustomerContactId', contactId, Attribute);
                }
            } else {
                console.log(`5. No DataCollection found`);
            }
        } catch (e) {
            console.log(e);
        } finally {
            client.close();
        }
    });


// init command
cmd.parse(process.argv);

/* private functions */

async function copyAttrs(client, tableId, srcSuffix, tgtSuffix, Attribute) {
    let attrs = await getAttributes(client, tableId, srcSuffix);
    for (let attr of attrs) {
        let attrName = attr.NAME;
        if (attrName.endsWith(srcSuffix)) {
            let dst = attrName.replace(srcSuffix, tgtSuffix);
            await copyAttr(client, tableId, attrName, dst, Attribute);
        }
    }
}

function attrsToDebugStr(attrs) {
    return attrs.map(attr => attr.NAME).join(',');
}

function getClient(conf) {
    return new Sequelize(`mysql://${conf.username}:${conf.password}@${conf.host}/${DATABASE}`, { logging: false });
}

async function getExtracts(client, tableIds) {
    let result = await client.query(`
SELECT * FROM METADATA_EXTRACT
WHERE FK_TABLE_ID in (${tableIds.join(',')})
`);
    if (!result || result.length === 0 || !result[0] || result[0].length === 0) {
        return [];
    }

    let path = {};
    for (let row of result[0]) {
        path[row.FK_TABLE_ID] = row.PATH;
    }
    return path;
}

async function getDataCollectionTables(client, collectionId, version) {
    let result = await client.query(`
SELECT * FROM METADATA_DATA_COLLECTION_TABLE
WHERE FK_COLLECTION_ID = ${collectionId}
AND VERSION = '${version}'
AND ROLE in ('ConsolidatedAccount', 'SystemAccount', 'ConsolidatedContact', 'SystemContact');
`);
    if (!result || result.length === 0 || !result[0] || result[0].length === 0) {
        return {};
    }

    let tables = {};
    for (let row of result[0]) {
        tables[row.ROLE] = row.FK_TABLE_ID;
    }
    return tables;
}

async function getDataCollection(client, tenant) {
    let result = await client.query(`
SELECT * FROM METADATA_DATA_COLLECTION
WHERE FK_TENANT_ID = (
    SELECT TENANT_PID FROM TENANT WHERE TENANT_ID = '${tenant}.${tenant}.Production'
);
`);
    if (!result || result.length === 0 || !result[0] || result[0].length === 0) {
        return null;
    }

    return result[0][0];
}

async function copyAttr(client, tableId, srcAttrName, targetAttrName, Attribute) {
    let attr = await getAttribute(client, tableId, srcAttrName);
    let targetAttr = await getAttribute(client, tableId, targetAttrName);
    if (!attr) {
        console.error(`Failed to copy src=${srcAttrName} to target=${targetAttrName}. Src attr does not exist in table ${tableId}`);
        return;
    }
    delete attr.PID;
    attr.NAME = targetAttrName;
    attr.DISPLAY_NAME = targetAttrName;
    if (!targetAttr) {
        let res = await Attribute.create(attr);
        console.log(`Copy src attr ${srcAttrName} to target attr ${targetAttrName}, PID=${res.PID}`);
    } else {
        console.log(`Target attribute ${targetAttrName} already exists in table ${tableId}. Attr = ${targetAttr.PID}`);
    }
}

async function addAccountId(client, accountTemplate, accId, renameDisplayName, Attribute) {
    let attr = await getAttribute(client, accountTemplate.FK_TEMPLATE_ID, 'CustomerAccountId');
    let targetAttr = await getAttribute(client, accountTemplate.FK_TEMPLATE_ID, accId);
    delete attr.PID;
    attr.NAME = accId;
    if (renameDisplayName) {
        attr.DISPLAY_NAME = accId;
    }

    if (!targetAttr) {
        let res = await Attribute.create(attr);
        console.log(`Added account id attribute ${accId}, PID=${res.PID}`);
    } else {
        console.log(`Account id attribute already exists in table ${accountTemplate.FK_TEMPLATE_ID}. Attr = ${targetAttr.PID}`);
    }
}

async function addContactId(client, contactTemplate, contactId, renameDisplayName, Attribute) {
    let attr = await getAttribute(client, contactTemplate.FK_TEMPLATE_ID, 'CustomerContactId');
    let targetAttr = await getAttribute(client, contactTemplate.FK_TEMPLATE_ID, contactId);
    delete attr.PID;
    attr.NAME = contactId;
    if (renameDisplayName) {
        attr.DISPLAY_NAME = contactId;
    }

    if (!targetAttr) {
        let res = await Attribute.create(attr);
        console.log(`Added contact id attribute ${contactId}, PID=${res.PID}`);
    } else {
        console.log(`Contact id attribute already exists in table ${contactTemplate.FK_TEMPLATE_ID}. Attr = ${targetAttr.PID}`);
    }
}

async function getAttributes(client, tableId, suffix) {
    let result = await client.query(`SELECT * FROM METADATA_ATTRIBUTE WHERE FK_TABLE_ID = ${tableId} AND NAME LIKE '%${suffix}';`);
    return result ? result[0] : [];
}

async function getAttribute(client, tableId, column) {
    let result = await client.query(`SELECT * FROM METADATA_ATTRIBUTE WHERE FK_TABLE_ID = ${tableId};`);
    for (let attr of result[0]) {
        if (attr.NAME === column) {
            return attr;
        }
    }
    return null;
}

async function getDefaultAccountTemplate(client, tenant) {
    return await getDefaultSystemTemplate(client, tenant, "AccountData");
}

async function getDefaultContactTemplate(client, tenant) {
    return await getDefaultSystemTemplate(client, tenant, "ContactData");
}

async function getDefaultSystemTemplate(client, tenant, templateName) {
    let result = await client.query(`
SELECT * FROM DATAFEED_TASK
WHERE FEED_TYPE = 'DefaultSystem_${templateName}'
AND FK_FEED_ID = (
    SELECT PID FROM DATAFEED WHERE FK_TENANT_ID = (
        SELECT TENANT_PID FROM TENANT WHERE TENANT_ID = '${tenant}.${tenant}.Production'
    )
);
`);
    let rows = result[0];
    return (rows && rows.length === 1) ?  rows[0] : null;
}

function checkSystem(system) {
    if (system.ACCOUNT_SYSTEM_ID) {
        throw new Error(`Already has system account ID set = ${system.ACCOUNT_SYSTEM_ID}`);
    }
    if (system.CONTACT_SYSTEM_ID) {
        throw new Error(`Already has system contact ID set = ${system.CONTACT_SYSTEM_ID}`);
    }
    if (system.MAP_TO_LATTICE_ACCOUNT[0]) {
        throw new Error(`Already mapped to lattice account`);
    }
    if (system.MAP_TO_LATTICE_CONTACT[0]) {
        throw new Error(`Already mapped to lattice contact`);
    }
}

function randomString(length) {
    let chars = '0123456789abcdefghijklmnopqrstuvwxyz';
    var result = '';
    for (var i = length; i > 0; --i) result += chars[Math.floor(Math.random() * chars.length)];
    return result;
}

function newSystemId(suffix) {
    return `user_DefaultSystem_${randomString(8)}_${suffix}`;
}

async function setAccountSystemId(client, systemPid, accId) {
    let result = await client.query(`
UPDATE ATLAS_S3_IMPORT_SYSTEM
SET ACCOUNT_SYSTEM_ID='${accId}'
WHERE PID = ${systemPid}
`);
    console.log(`2-1. Set system account id to ${accId}, result=${result[0].affectedRows}`);
}

async function setContactSystemId(client, systemPid, contactId) {
    let result = await client.query(`
UPDATE ATLAS_S3_IMPORT_SYSTEM
SET CONTACT_SYSTEM_ID='${contactId}'
WHERE PID = ${systemPid}
`);
    console.log(`2-2. Set system contact id to ${contactId}, result=${result[0].affectedRows}`);
}

async function mapToLatticeEntity(client, systemPid) {
    let result = await client.query(getMapToLatticeEntityQuery(systemPid));
    console.log(`2-3. Map to lattice entity result=${result[0].affectedRows}`);
}

async function getDefaultSystem(client, tenant) {
    let result = await client.query(getDefaultSystemQuery(tenant));
    let systems = result[0];
    if (!systems || systems.length !== 1 || !systems[0]) {
        throw new Error(`Default system should have exactly one: ${systems}`);
    }

    return systems[0];
}

function getMapToLatticeEntityQuery(systemPid) {
    return `
UPDATE ATLAS_S3_IMPORT_SYSTEM
SET MAP_TO_LATTICE_ACCOUNT=1, MAP_TO_LATTICE_CONTACT=1
WHERE PID = ${systemPid}
`;
}

function getDefaultSystemQuery(tenant) {
    return `
SELECT * FROM ATLAS_S3_IMPORT_SYSTEM
WHERE FK_TENANT_ID = (
    SELECT TENANT_PID FROM TENANT WHERE TENANT_ID = '${tenant}.${tenant}.Production'
)
AND NAME = 'DefaultSystem';
`;
}

