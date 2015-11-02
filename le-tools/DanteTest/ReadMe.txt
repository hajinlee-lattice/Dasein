For this project, we need install python 2.7 and following packages need be installed for python
==============================================
1. py-dom-xpath (this should be installed manually, the install file be placed in Bin folder)
2. pyodbc
3. PyYAML
4. robotframework-seleninum2library
5. selenium
6, Chromewebdriver.exe should be placed to the python folder (c:\python2.7) this exe file is needed if we want to use chrome to test web UI automated. I have downloaed the file and placed it in the Bin folder.




Following are the command to run test cases 
===========================================
1. command for DanteUISmoke test in SFDC:
	python -m unittest Tests.DanteUISmoke   (this command should be executed under the folder "DanteTest")

2. command for PRODDanteUISmoke test:
	python -m unittest Tests.ProdDanteUISmoke   (this command should be executed under the folder "DanteTest")


Most Test server and account information can be configured in the config.ini and Property.py, please make sure these configuration are correct before you run tests.	
we can execute setupproperty.py to set configuration in config.ini