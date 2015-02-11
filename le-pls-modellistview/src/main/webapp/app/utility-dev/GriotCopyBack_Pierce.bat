@echo off

rem --------------------------------------------------------
rem CHANGE THESE FOR YOUR LOCAL SETTINGS AND MAKE A NEW FILE
rem --------------------------------------------------------

echo Ryan Pierce (rpierce) 8/10/2012
set batchFilePath="C:\branches\LLL_early_cut\Griot\Projects\GriotWebSite\app\utility-dev"
set src="C:\websites\griot"
set dst="C:\branches\LLL_early_cut\Griot\Projects\GriotWebSite"

rem --------------------------------------------------------

rem change to the path where the copy script is in case running from another directory (i.e. from aptana)
cd %batchFilePath%

call CopyBackJavascriptToGriotWebsite %src% %dst%

set /p variable = "Hit Enter to close."