# Open a terminal on Mac
# Go into the directory where this file is
# Make sure there is a Code folder with the requirements.txt file
# Update this file with your chosen venv name
# Type *make* into your terminal
# Then type *conda activate [your environment]*
# Then type *make jupyter* to install jupyter notebooks into you venv and attach the kernel for your venv
# Your terminal should then have the venv name on each line instead of (base)

VENV = .venv_default
PYTHON = $(VENV)/bin/python3
PIP = $(VENV)/bin/pip
REQ = Code/requirements.txt

run: $(VENV)/bin/activate
	#$(PYTHON) dummy.py


$(VENV)/bin/activate: $(REQ)
	#python3 -m venv $(VENV)
	conda create -n $(VENV) python
	#$(PIP) install -r requirements.txt
	conda install --yes --file $(REQ)
	conda activate $(VENV)


activate:
	conda activate $(VENV)

clean:
	rm -rf __pycache__
	rm -rf $(VENV)

# run this after activating the environment
jupyter:
	conda install jupyter
	ipython kernel install --name $(VENV) --user