
TEMPLATE=chimebot-whatsnew.yml
FINAL_TEMPLATE=chimebot-whatsnew-release.yml
CF_STACK=ChimeBot-Whats-New-At-AWS

include properties.mk

all: build deploy

build:
	make -C function
	aws cloudformation package \
		--template-file $(TEMPLATE) \
		--s3-bucket $(ARTIFACT_BUCKET) \
		--output-template-file $(FINAL_TEMPLATE)

deploy: all
	aws cloudformation deploy \
		--region $(AWS_REGION) \
		--template-file $(FINAL_TEMPLATE) \
		--stack-name $(CF_STACK) \
		--capabilities CAPABILITY_IAM \
		--parameter-overrides 'ChatBotURL=$(CHAT_URL)'

