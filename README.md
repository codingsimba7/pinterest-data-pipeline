# Pinterest Data Pipeline - Data Engineering Capstone Project

## Table of Contents
1. [Project Description](#project-description)
2. [Installation](#installation)
   - [Environment Setup](#environment-setup)
   - [Kafka Configuration](#kafka-configuration)
   - [MSK to S3 Connection](#msk-to-s3-connection)
   - [API Configuration in API Gateway](#api-configuration-in-api-gateway)
   - [Batch Processing Databricks](#batch-processing-databricks)
   - [Automating Batch Processing with AWS MWAA](#automating-batch-processing-with-aws-mwaa)
3. [Usage](#usage)
    - [Loading Data](#loading-data)
4. [Contributing](#contributing)
5. [License](#license)

## Project Description
Pinterest processes billions of data points every day to optimize the value delivered to its users. In this data engineering capstone project, a similar system is emulated leveraging AWS Cloud, Kafka, Airflow, and Spark.


## Installation

### Environment Setup
1. Establish an EC2 instance and an MSK cluster.
2. Create a `.pem` file containing the key pair associated with the EC2 instance for connecting to the instance using an SSH client.
3. Connect the EC2 instance to the MSK cluster.
4. Install Java 8 on the cluster by executing `sudo yum install java-1.8.0` in the terminal connected to EC2.

### Kafka Configuration
1. Download Kafka onto the EC2 client:
    ```bash
   wget https://archive.apache.org/dist/kafka/2.8.1/kafka_2.12-2.8.1.tgz
   tar -xzf kafka_2.12-2.8.1.tgz
    ```
2. Download the IAM MSK authentication package into the libs section of the Kafka folder:
    ```bash
   wget https://github.com/aws/aws-msk-iam-auth/releases/download/v1.1.5/aws-msk-iam-auth-1.1.5-all.jar
    ```
3. Set up environment variable CLASSPATH:
    ```bash
   export CLASSPATH=/home/ec2-user/kafka_2.12-2.8.1/libs/aws-msk-iam-auth-1.1.5-all.jar
    ```
   To automate this, add the command to the `.bashrc` file.

4. Configuration details for the `clients.properties` file:
    ```properties
    # Sets up TLS for encryption and SASL for authN.
    security.protocol = SASL_SSL

    # Identifies the SASL mechanism to use.
    sasl.mechanism = AWS_MSK_IAM

    # Binds SASL client implementation.
    sasl.jaas.config = software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn="arn:aws:iam::584739742957:role/12256357c821-ec2-access-role";

    # Encapsulates constructing a SigV4 signature based on extracted credentials.
    # The SASL client bound by "sasl.jaas.config" invokes this class.
    sasl.client.callback.handler.class = software.amazon.msk.auth.iam.IAMClientCallbackHandler
    ```

5. Create Topics:
    ```bash
    ./kafka-topics.sh --bootstrap-server BootstrapServerString --command-config client.properties --create --topic <topic_name>
    ```
    Three topics for this project are `pin`, `geo`, and `user`.

### MSK to S3 Connection
1. IAM role configuration:
    - Create policy and edit the JSON:
        ```json
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "VisualEditor0",
                    "Effect": "Allow",
                    "Action": [
                        "s3:ListBucket",
                        "s3:DeleteObject",
                        "s3:GetBucketLocation"
                    ],
                    "Resource": [
                        "arn:aws:s3:::<DESTINATION_BUCKET>",
                        "arn:aws:s3:::<DESTINATION_BUCKET>/*"
                    ]
                },
                {
                    "Sid": "VisualEditor1",
                    "Effect": "Allow",
                    "Action": [
                        "s3:PutObject",
                        "s3:GetObject",
                        "s3:ListBucketMultipartUploads",
                        "s3:AbortMultipartUpload",
                        "s3:ListMultipartUploadParts"
                    ],
                    "Resource": "*"
                },
                {
                    "Sid": "VisualEditor2",
                    "Effect": "Allow",
                    "Action": "s3:ListAllMyBuckets",
                    "Resource": "*"
                }
            ]
        }
        ```
    - Edit Trust relationships and add the following trust policy:
        ```json
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "kafkaconnect.amazonaws.com"
                    },
                    "Action": "sts:AssumeRole"
                }
            ]
        }
        ```

2. On the EC2 client download the Confluent.io Amazon S3 Connector and copy it to the S3 bucket created for this project.
    ```bash
   sudo -u ec2-user -i
   mkdir kafka-connect-s3 && cd kafka-connect-s3
   wget https://d1i4a15mxbxib1.cloudfront.net/api/plugins/confluentinc/kafka-connect-s3/versions/10.0.3/confluentinc-kafka-connect-s3-10.0.3.zip
   aws s3 cp ./confluentinc-kafka-connect-s3-10.0.3.zip s3://<BUCKET_NAME>/kafka-connect-s3/
    ```
3. Create a custom plugin in the MSK Connect console.
    - Navigate to the MSK console, select custom plugins, and create a custom plugin.
4. Create the connector.
    - Navigate to the MSK console, select connectors, and create a connector.
    - Connector Configuration settings:
       ```
       connector.class=io.confluent.connect.s3.S3SinkConnector
       s3.region=us-east-1
       flush.size=1
       schema.compatibility=NONE
       tasks.max=3
       topics.regex=<YOUR_UUID>.*
       format.class=io.confluent.connect.s3.format.json.JsonFormat
       partitioner.class=io.confluent.connect.storage.partitioner.DefaultPartitioner
       value.converter.schemas.enable=false
       value.converter=org.apache.kafka.connect.json.JsonConverter
       storage.class=io.confluent.connect.s3.storage.S3Storage
       key.converter=org.apache.kafka.connect.storage.StringConverter
       s3.bucket.name=<BUCKET_NAME>
       ```

   Leave other configurations as default, except for:
    - Worker Configuration: select "Use a customised configuration", then pick "confluent-worker".
    - Access permissions: select the previously created IAM role.

### API Configuration in API Gateway
1. Navigate to API Gateway and create a new API.
   - Select REST API.
   - Use the HTTP ANY method.
   - Set the Endpoint URL to the PublicDNS from the EC2 instance.
2. Deploy the API and note the Invoke URL.
3. Install the Confluent package for the Kafka REST proxy on your EC2 client machine.
4. Allow the REST proxy to perform IAM authentication to the MSK cluster by modifying the `kafka-rest.properties` file.

### Batch Processing Databricks
1. Mount the S3 bucket to the Databricks workspace. Refer to Create_Mount Notebook
2. Load and Tranform the data. Refer to data_transformation Notebook Note: this is run on your databricks account

### Automating Batch Processing with AWS MWAA
1. Orchestrate Databricks Workloads on AWS MWAA
    - create a S3 bucket for MWAA, and create a folder for the DAGs called dags. Configuration: Block all public access and have bucket versioning enabled.
2. Create a MWAA environment:
    - Under Dag code in amazon s3 choose the bucket and folder created in the previous step.
    - Under Networking page, chose create mwaa vpc to automatically create a vpc for the environment.
    - Apache Airflow access mode: Public or Private Selecting a *private network* will limit access to the UI to users within the MWAA VPC. Selecting a *public network* will allow the UI to be accessed over the internet.
    - Under Security groups, select Create new security group, which will allow MWAA to create a VPC security group within inbound and outbound rules based on the type of web server access selected.
    - Under Environment class select the desired class, as well as the minimum and maximum worker count. It's recommended to choose the smallest environment size that is necessary to support the workload.
    - Leave the rest of default options and choose next. Finally choose Create environment.
3. Create an API Token in Databricks
    - Navigate to the User Settings page.
    - Select the Access Tokens tab.
    - Click the Generate New Token button.
    - Enter a description for the token.
    - Select the Generate button.
    - Copy the generated token and store it in a secure location.
4. Create the MWAA to Databricks connection
    - Navigate to the environemnts page in the MWAA console.
    - Select the environment created in the previous step.
    - Open Airflow UI 
    - Navigate to Admin > Connections
    - Select databricks_default and edit record
    - Enter the following values:
        - Conn Type: Databricks
        - Host: In the Host column copy and paste the url of your Databricks account. You can find this by simply navigating your account and copying the URL in the top bar.
        - Extra: {"token": "<token_from_previous_step>", "host": "<url_from_host_column>"}
5. Create and upload requirements.txt file
    - use the following Github repository for that: https://github.com/aws/aws-mwaa-local-runner.
    - clone the repository git clone 
    - cd aws-mwaa-local-runner
    - ./mwaa-local-env build-image
    - ./mwaa-local-env start
    - open http://localhost:8080 in your browser
    - username: admin and password: test
    - to add the desired Python dependencies you will need to navigate to aws-mwaa-local-runner/requirements/. Inside this folder is where you will create your requirements.txt file
    - Inside requirements.txt you should add the following line to install the package needed for the Databricks connection type: apache-airflow[databricks]
    - upload the requiremnts.txt file to the s3 bucket created for MWAA
    - Navigate to the MWAA console and select your Environment. Once you're on the environment page select Edit. Under the DAG code in Amazon S3, update your Requirements file field by selecting the path corresponding to the requirements.txt file you have just uploaded to the S3 bucket.
6. Create the Airflow DAG - refer to _dag.py file

## Usage

### Loading Data
1. Make sure to have set up everything up to [API Configuration in API Gateway](#api-configuration-in-api-gateway)
2. Run the API Gateway 
3. Run user_posting_emulation.py file

### Transforming Data
1. Make sure to have set up everything up to [Batch Processing Databricks](#batch-processing-databricks)
2. The data was loaded and transformed using the Databricks notebook- refer to Data_Transformation notebook

### Querying Data
1. Refer to SQL Queries notebook

## License