## Setup

Install [Maven](http://maven.apache.org/).

[Authenticate using a service account](https://cloud.google.com/docs/authentication/getting-started).
Create a service account, download a JSON key file, and set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable.

## Build:

	mvn clean package

## Run 
mvn exec:java -Dexec.mainClass=com.dump.bigquery.ServianMain 
