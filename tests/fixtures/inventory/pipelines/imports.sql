-- pipeline: imports | state: Stopped | source: S3 | target: table events
CREATE PIPELINE `imports` AS LOAD DATA S3 'example-bucket/imports/' CONFIG '{"region": "us-west-2"}' CREDENTIALS '<<CREDENTIALS>>' INTO TABLE `events` FORMAT CSV;
