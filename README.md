# ChimeBot-Whats-New-At-AWS

A ChatBot for Amazon Chime that sends a message to a group for each news from AWS.

Made with ❤️ by François-Xavier Gsell. Available on the [AWS Serverless Application Repository](https://aws.amazon.com/serverless)

## MANUAL INSTALLATION

### Requirements

- Python 3.x
- pip3

### Step by Step

1. Copy `properties.dummy.mk` to `properties.mk`
1. Set a value for `ARTIFACT_BUCKET`, `AWS_REGION` and `CHAT_URL` in  `properties.mk`
    - `AWS_REGION`: The AWS region where the bot will be deployed
    - `ARTIFACT_BUCKET`: The S3 bucket to store the package for Lambda, must be in the same `AWS_REGION`
    - `CHAT_URL`: The webhook URL for your Amazon Chime group.
    - `FIREHOSE_STREAM`: Then name of a Stream in the same region to archive the news.
1. Run `make`

## License

MIT License