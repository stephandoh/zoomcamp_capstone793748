# AWS Setup Guide

This guide walks you through setting up all the AWS infrastructure required to run the VoltStream pipeline. You will need an AWS account before starting.

The services you will configure are:

- **S3** — two buckets for Bronze and Silver data storage
- **IAM** — a user with the correct permissions to access S3 and Redshift
- **Redshift Serverless** — the analytical warehouse where dbt models are built

## API Keys

Before touching AWS you need two external API keys. The pipeline will not run without them.

### Open Charge Map

1. Go to [https://openchargemap.org/site/develop/api](https://openchargemap.org/site/develop/api)
2. Click **Register** and create a free account
3. Once logged in, navigate to your profile and find **API Key**
4. Copy the key and add it to your `.env` file as `OPENCHARGE_API_KEY`

The free tier has no hard rate limit but be respectful — the pipeline uses `time.sleep(0.5)` between pagination calls by default.

### OpenWeatherMap

1. Go to [https://openweathermap.org/api](https://openweathermap.org/api)
2. Click **Sign Up** and create a free account
3. Navigate to **API Keys** in your account dashboard
4. A default key is generated automatically — copy it
5. Add it to your `.env` file as `WEATHER_API_KEY`

The free tier allows 60 API calls per minute. The pipeline fetches one call per weather zone with a 1 second delay between calls, staying well within this limit.

## S3 — Create Two Buckets

You need two S3 buckets — one for raw Bronze data and one for cleaned Silver data.

### Creating the Bronze bucket

1. Open the AWS Console and navigate to **S3**
2. Click **Create bucket**
3. Set **Bucket name** to `voltstream-bronze`
4. Set **AWS Region** to `us-east-1` (or your preferred region — just keep it consistent across all services)
5. Leave **Block all public access** enabled — this bucket should never be public
6. Leave all other settings as default
7. Click **Create bucket**

### Creating the Silver bucket

Repeat the same steps with **Bucket name** set to `voltstream-silver`.

Your two buckets will receive data in this structure:

```
voltstream-bronze/
├── ev/YYYY/MM/DD/ev_stations_TIMESTAMP.json
├── weather/YYYY/MM/DD/weather_TIMESTAMP.json
└── state/
    ├── ev_stations_last_ingestion.json
    └── weather_last_ingestion.json

voltstream-silver/
├── ev/YYYY/MM/DD/silver_ev_TIMESTAMP.json
└── weather/YYYY/MM/DD/silver_weather_TIMESTAMP.json
```

## IAM — Create a User with Correct Permissions

The pipeline authenticates to AWS using access keys. You need an IAM user with the right permissions attached.

### Creating the IAM user

1. Navigate to **IAM** in the AWS Console
2. Click **Users** in the left sidebar
3. Click **Create user**
4. Set **User name** to `voltstream-pipeline`
5. Click **Next** — do not enable Console access, this user is for programmatic access only
6. On the permissions screen select **Attach policies directly**
7. Search for and attach the following managed policies:
   - `AmazonS3FullAccess` — allows reading and writing to your S3 buckets
   - `AmazonRedshiftFullAccess` — allows connecting to Redshift and executing statements
8. Click **Next** then **Create user**

### Generating access keys

1. Click on the user you just created
2. Navigate to the **Security credentials** tab
3. Scroll to **Access keys** and click **Create access key**
4. Select **Application running outside AWS** as the use case
5. Click **Next** then **Create access key**
6. Copy both the **Access key ID** and **Secret access key** immediately — the secret key is only shown once
7. Add them to your `.env` file:

```dotenv
AWS_ACCESS_KEY_ID=your_access_key_id
AWS_SECRET_ACCESS_KEY=your_secret_access_key
```

### Adding inline policies for S3 bucket access

For tighter security you can optionally add inline policies that restrict access to only your VoltStream buckets. Navigate to your IAM user, click **Add permissions**, then **Create inline policy** and add one policy for each bucket:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::voltstream-bronze",
        "arn:aws:s3:::voltstream-bronze/*",
        "arn:aws:s3:::voltstream-silver",
        "arn:aws:s3:::voltstream-silver/*"
      ]
    }
  ]
}
```

## Redshift Serverless — Create a Workgroup

Redshift Serverless is the warehouse layer where your dbt models are built. You do not need to manage any clusters or capacity — AWS handles this automatically.

### Creating a namespace

1. Navigate to **Amazon Redshift** in the AWS Console
2. In the left sidebar click **Serverless dashboard**
3. Click **Create workgroup**
4. On the first screen set **Workgroup name** to `default-workgroup`
5. Leave the base capacity at the default (8 RPUs)
6. Click **Next**

### Configuring the namespace

1. Select **Create a new namespace**
2. Set **Namespace name** to `default-namespace`
3. Under **Database name** enter `dev`
4. Under **Admin user credentials** select **Customize admin user credentials**
5. Set **Admin user name** to `admin`
6. Under **Admin password** select **Manually add the admin password**
7. Choose a strong password and note it down — you will need it in your `.env` file and `profiles.yml`

> **Important:** Do not select "Generate a password" — AWS will rotate this automatically and break your dbt connection. Always use "Manually add the admin password" so you control when it changes.

8. Click **Next**

### Network and security settings

1. Under **Network and security** enable **Turn on publicly accessible**
2. Under **VPC security group** ensure the default security group is selected
3. You need to add an inbound rule to allow Redshift connections. Navigate to **EC2 → Security Groups**, find the default security group, and add:
   - **Type:** Custom TCP
   - **Port range:** 5439
   - **Source:** Your IP address (or `0.0.0.0/0` for unrestricted access — not recommended for production)
4. Click **Save rules**

### Completing the workgroup setup

1. Review your settings and click **Create**
2. Wait 2-3 minutes for the workgroup to become available
3. Once available, click on the workgroup and note the **Endpoint** — it will look like:
   `default-workgroup.ACCOUNTID.us-east-1.redshift-serverless.amazonaws.com`
4. Add this to your `.env` file as `DBT_HOST`

### Creating the IAM role for S3 access

Redshift needs its own IAM role to read from S3 during COPY commands. This is separate from the user credentials you created earlier.

1. Navigate to **IAM → Roles**
2. Click **Create role**
3. Select **AWS service** as the trusted entity
4. Search for and select **Redshift** then choose **Redshift - Customizable**
5. Click **Next**
6. Attach the policy `AmazonS3ReadOnlyAccess`
7. Name the role `AmazonRedshift-CommandsAccessRole`
8. Click **Create role**
9. Copy the **Role ARN** — you will use this in the COPY commands when loading data into Redshift

### Attaching the IAM role to your workgroup

1. Navigate back to **Redshift Serverless → Namespaces**
2. Click on `default-namespace`
3. Go to the **Security** tab
4. Under **IAM roles** click **Manage IAM roles**
5. Select the role you just created and click **Save changes**

### Granting permissions in Redshift

After connecting to Redshift for the first time you need to grant your user permissions on the schemas the pipeline creates. Run these statements in the Redshift Query Editor v2:

```sql
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS gold;

GRANT ALL ON DATABASE dev TO admin;
GRANT ALL ON SCHEMA public TO admin;
GRANT ALL ON SCHEMA staging TO admin;
GRANT ALL ON SCHEMA gold TO admin;

ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL ON TABLES TO admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT ALL ON TABLES TO admin;
```

If you are connecting via Airflow using IAM credentials, find the IAM user name Redshift maps your session to by running `SELECT current_user;` and grant the same permissions to that user:

```sql
GRANT ALL ON DATABASE dev TO "IAM:your-iam-username";
GRANT ALL ON SCHEMA public TO "IAM:your-iam-username";
GRANT ALL ON SCHEMA staging TO "IAM:your-iam-username";
GRANT ALL ON SCHEMA gold TO "IAM:your-iam-username";
```

### Creating the source tables

Before running the pipeline for the first time, create the two source tables in the `public` schema that Redshift COPY commands write to:

```sql
CREATE TABLE IF NOT EXISTS public.ev_stations (
    station_id                INTEGER,
    uuid                      VARCHAR(100),
    operator_id               INTEGER,
    operator_name             VARCHAR(255),
    usage_type_id             INTEGER,
    status_type_id            INTEGER,
    number_of_points          INTEGER,
    is_recently_verified      BOOLEAN,
    date_created              VARCHAR(30),
    date_last_verified        VARCHAR(30),
    date_last_status_update   VARCHAR(30),
    title                     VARCHAR(255),
    address_line1             VARCHAR(255),
    address_line2             VARCHAR(255),
    town                      VARCHAR(100),
    state_or_province         VARCHAR(100),
    postcode                  VARCHAR(20),
    country_id                INTEGER,
    country_iso_code          VARCHAR(10),
    latitude                  FLOAT,
    longitude                 FLOAT,
    related_url               VARCHAR(500),
    connection_id             INTEGER,
    connection_type_id        INTEGER,
    connection_type_name      VARCHAR(255),
    connection_status_type_id INTEGER,
    level_id                  INTEGER,
    level_description         VARCHAR(255),
    amps                      FLOAT,
    voltage                   FLOAT,
    power_kw                  FLOAT,
    current_type_id           INTEGER,
    current_type_description  VARCHAR(100),
    quantity                  INTEGER,
    is_operational            BOOLEAN,
    lat_round                 FLOAT,
    lon_round                 FLOAT,
    data_quality_error        BOOLEAN,
    valid_from                VARCHAR(30),
    valid_to                  VARCHAR(30),
    is_current                BOOLEAN
);

CREATE TABLE IF NOT EXISTS public.weather (
    lat_round             FLOAT,
    lon_round             FLOAT,
    temperature_c         FLOAT,
    temperature_f         FLOAT,
    condition             VARCHAR(100),
    condition_description VARCHAR(255),
    wind_speed            FLOAT,
    humidity              FLOAT,
    ingested_at           VARCHAR(50),
    processed_at          VARCHAR(50)
);
```

## Environment Variables Summary

Once all AWS services are set up your `.env` file should contain:

```dotenv
OPENCHARGE_API_KEY=your_open_charge_map_key
WEATHER_API_KEY=your_openweathermap_key
COUNTRY_CODE=US
S3_BUCKET_BRONZE=voltstream-bronze
S3_BUCKET_SILVER=voltstream-silver
AWS_ACCESS_KEY_ID=your_iam_access_key
AWS_SECRET_ACCESS_KEY=your_iam_secret_key
REDSHIFT_PASSWORD=your_redshift_admin_password
DBT_HOST=default-workgroup.ACCOUNTID.us-east-1.redshift-serverless.amazonaws.com
DBT_DATABASE=dev
DBT_USER=admin
DBT_PASSWORD=your_redshift_admin_password
```

With all of this in place you are ready to move to the [Local Setup Guide](02_local_setup.md).
