For this project, we need install python 2.7 and following packages need be installed for python
==============================================
1. py-dom-xpath (this should be installed manually, the install file be placed in Bin folder)
2. pyodbc
3. PyYAML
4. robotframework-seleninum2library
5. selenium
6, Chromewebdriver.exe should be placed to the python folder (c:\python2.7) this exe file is needed if we want to use chrome to test web UI automated. I have downloaed the file and placed it in the Bin folder.




There are 3 py files for Dante UI cases:
=========================================
1.DanteUISmoke.py ---- In this file there are 2 Test class 
	a. LeadInSFDCTest ----This class contains 6 cases for Leads Dante UI
	b. AccountInSFDCTest---- This class contains 6 cases for Account Dante UI

2. DanteUIFuntional.py -------There is 1 test class now
	a. AccountInSFDCTest ------In this class, Some Functional cases for Account Dante UI 1.6.2 are created

3. ProdDanteUISmoke.py ------Cases in this file are created for PROD somke verification
	a. DanteDemoURLTest ---- somke cases for the PROD Dante UI by Dante Demo URL directly
	b. DanteInSFDCTest ---- somke cases for the PROD Dante UI by Login SFDC to access Dante UI


Following are the command to run test cases 
===========================================
1. command for DanteUISmoke test in SFDC:
	python -m unittest Dante.Tests.DanteUISmoke   (this command should be executed under the folder "LE-Tools")

2. command for PRODDanteUISmoke test:
	python -m unittest Dante.Tests.ProdDanteUISmoke   (this command should be executed under the folder "LE-Tools")


Most Test server and account information can be configured in the config.ini and Property.py, please make sure these configuration are correct before you run tests.	
we can execute setupproperty.py to set configuration in config.ini