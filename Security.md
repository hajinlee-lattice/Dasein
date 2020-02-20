# Security overview file for Atlas (aka D&B Lattice) in the ledp (Lattice Engines Data Platform) GitHub repository.
## Security contacts
* Jim Talbot, Security Program Manager, talbotj@dnb.com
## Security configuration
* Confidentiality: Encryption in-transit (TLS 1.2) and at-rest (AES-256).
* Integrity: Access to customer data restricted to the necessary Operations staff.
* Availability: The SLA is 99.95% with the Disaster Recovery backup in a different AWS Region.
* Authentication: Login directly at https://app.lattice-engines.com/login/form or using SSO.
* Authorization: Access and roles are managed by customer administrators. The privileges for each role are determined by the product.
* Auditing: User logins and API transactions are logged to Splunk. AWS actions are also in CloudTrail.
## Application release information
* Major Releases are every six weeks. Milestone 34 (M34) was released on 2/13/2020.
* Minor Releases are typically two weeks after the major releases.
## Known security issues, gaps & future enhancements
* Refer to the Open Application Issues tab in https://jira.aws.dnb.com/browse/SAA-29137.
