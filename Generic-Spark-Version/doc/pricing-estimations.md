# Pricing estimations for data lake component

### Description 

This page will try to deliver updating estimation regarding the pricing of each componenet in the data lake architecture

Pricing currently refering to only on-demand plan - more detail properties will be updated (instance type for RDS and S3 storage and type)

## Services from AWS Calulator
This table is based on this report: [AWS Calulator](https://calculator.s3.amazonaws.com/index.html#r=CMH&key=files/calc-8bd777b34a830f6db20cae1ad39b9899b2d83350&v=ver20190718a0)

| Service Type                        | Components                    | Region                 | Component Price | Service Price |
|-------------------------------------|-------------------------------|------------------------|-----------------|---------------|
| Amazon S3 Service (US East (Ohio))  |                               |                        |                 | $431.54       |
|                                     | S3 Standard Storage:          | US East (Ohio)         | $47.11          |               |
|                                     | S3 Standard Put Requests:     | US East (Ohio)         | $0.03           |               |
|                                     | S3 Standard Other Requests:   | US East (Ohio)         | $0.40           |               |
|                                     | S3 Standard - IA Storage:     | US East (Ohio)         | $384            |               |
| Amazon RDS Service (US East (Ohio)) |                               |                        |                 | $1,461.92     |
|                                     | DB instances:                 | US East (Ohio)         | $1,359.52       |               |
|                                     | Storage:                      | US East (Ohio)         | $102.40         |               |
|                                     | I/O:                          | US East (Ohio)         | $0              |               |
| Amazon SNS Service (US East (Ohio)) |                               |                        |                 | $0            |
|                                     | Requests:                     | US East (Ohio)         | $0              |               |
|                                     | Notifications:                | US East (Ohio)         | $0              |               |
| AWS Data Transfer In                |                               |                        |                 | $0            |
|                                     | US East (Ohio) Region:        | Global                 | $0              |               |
| AWS Data Transfer Out               |                               |                        |                 | $17.91        |
|                                     | US East (Ohio) Region:        | Global                 | $17.91          |               |
| AWS Support (Business)              |                               |                        |                 | $191.14       |
|                                     | Support for all AWS services: |                        | $191.14         |               |
|                                     |                               | Total Monthly Payment: |                 | $2,102.51     |

## AWS Lambda will probably be almost free

## AWS ElasticSearch service

| price per hour | price per GB | amont of storage in GB | monthly pay |
|----------------|--------------|------------------------|-------------|
| 0.142          | 0.135        | 1024                   | 240.48      |

## AWS Glue

| Compute Hourly price  | Time will run (hours) | monthly pay |
|-----------------------|-----------------------|-------------|
| 0.44                  | 12                    | 158.4       |
