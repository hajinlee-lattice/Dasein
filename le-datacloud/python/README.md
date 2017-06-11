# Python Project for Lattice Data Cloud

This project contains scripts/modules required for some datacloud operations. 
Mainly bootstrapping DB tables.

## Objective

It may extend to more functionality in future. 
But as long as an operation can be done in the java project, 
we should avoid using python.

## Development Guide

This project should be self-contained.

### Conda Environment

Please use the pre-configured Anaconda environment `datacloud` 
to use/develop this project. 
You can bootstrap and update this environment by running the 
`setupenv_anaconda_datacloud.sh` in `le-dev/scripts`.

To activate this environment, use:

    source activate datacloud

To deactivate this environment, use:

    source deactivate datacloud

(Make sure `${ANACONDA_HOME}/bin` is in your `$PATH`)

### MySQL Schema

Need a LDC_ConfigDB in your local mysql. 
Use following script to create it

    DROP SCHEMA IF EXISTS `LDC_ConfigDB`;
    CREATE SCHEMA IF NOT EXISTS `LDC_ConfigDB`;
    GRANT ALL ON LDC_ConfigDB.* TO root@localhost;
    USE `LDC_ConfigDB`;
    
### Executable Interface

All cli tools are exposed via bin/datacloud. 
Make the file "bin/datacloud" executable.

    chmod +x bin/datacloud

Then you can check usage by

    datacloud -h
    
(It is linked into `$PATH` by the anaconda environment.)
In future, we will make this python project portable with this `datacloud` command.

For every module needs an executable interface, 
we create a `*_x.py` wrapper module that
contains only arguments definitions and hooks up to a backend module.

### Test

Add tests to `tests` module. Run them by

    pytest
    
under project root.