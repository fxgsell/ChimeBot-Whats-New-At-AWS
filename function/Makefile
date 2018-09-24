PIP=pip3
PACKAGE=chimebot-whatsnew.zip
ZIP=zip

all: deps
	$(ZIP) -r $(PACKAGE) *

deps:
	$(PIP) install -t . requests feedparser bs4
