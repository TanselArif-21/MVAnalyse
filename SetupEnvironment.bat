rem run 'where anaconda' and save the resulting path into @ana
@echo off
for /f %%i in ('where conda') do set @ana=%%i

rem set p_dir to be a string activating the conda environment
@echo off
set _path=%@ana%
for %%a in ("%_path%") do set "p_dir=%%~dpa"

SET p_dir=%p_dir%activate

rem activate the conda environment
call %p_dir%

if %errorlevel% neq 0 (
	echo Conda is not in path variable. Trying common location 1: C:\Users\%USERNAME%\AppData\Local\Continuum\anaconda3\Scripts\activate
	call C:\Users\%USERNAME%\AppData\Local\Continuum\anaconda3\Scripts\activate
) else (
	echo Success! Found Activate file!
)

if %errorlevel% neq 0 (
	echo Common location 1 not present. Trying common location 2: C:\Users\%USERNAME%\anaconda3\Scripts\activate
	call C:\Users\%USERNAME%\anaconda3\Scripts\activate
) else (
	echo Success! Found Activate file!
)

if %errorlevel% neq 0 (
	echo Could not find activate file in anaconda path
) else (
	echo Success! Found Activate file!
)

rem install virtualenv
call pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org virtualenv

rem create a virtual environment in the code folder
rem call virtualenv Code\venv
call python -m venv Code\venv

rem activate this new virtual environment
call Code\venv\Scripts\activate

rem if there is an error at this point, don't install anything
if %errorlevel% neq 0 (
	echo There was an error activating virtual environment. Exiting without installing into venv
	exit
)

rem install the accompanying requirements file
call pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org -r Code\requirements.txt

rem install jupyter into the virtual environment. Then install ipython, then add the virtual environment as a kernel in the local Jup Noteb
rem call pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org jupyter
rem call pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org ipython
rem call ipython kernel install --user --name=venv

rem run the python file of your choice
rem call python Code\[name of file].py

rem install the visualisation package
rem call pip install python-visualisation-formatting/.

rem leave the window open
cmd /k
