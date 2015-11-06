@echo off

rem Batch Script to copy files back from a published directory back to the repository directory
rem i.e. if you are editing javascript files in PoetLocal but want to copy them back to LMWebsite in order to check them in
rem Author: Chris Kwan (ckwan)
rem Last Update: 8/17/2012

echo -----
echo NOTE: this does NOT delete files from the destination directory, you will have to do that manually.
echo -----

set src=%1
set dst=%2

rem Prompt for confirmation
echo -----
echo Copying from [[%src%]] to [[%dst%]].
echo -----
set /p continue=Continue? (y/n): 
if %continue% neq y (exit)

set subdir="assets"
call:copyFunction

set subdir="app"
call:copyFunction

set subdir="index.aspx"
call:copyFunction

goto:eof

rem -----
rem function section
rem -----

:copyFunction
rem xcopy parameters
rem (ref: http://www.microsoft.com/resources/documentation/windows/xp/all/proddocs/en-us/xcopy.mspx?mfr=true)
rem /s - copy subdirectories
rem /e - copy empty directories
rem /y - don't prompt for confirmation when overwriting files
rem /d - only copy source files that are newer than destination files (i.e. updated files)
echo Copying %subdir%
xcopy %src%\%subdir% %dst%\%subdir% /s /e /y /d
goto:eof