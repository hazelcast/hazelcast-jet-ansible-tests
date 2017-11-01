# Hazelcast Jet Soak Tests

This repository contains a set of tests for
[Hazelcast Jet Soak Testing Environment](https://github.com/hazelcast/hazelcast-jet-ansible). Instructions on how to run the test suite and set up the environment are there. 

The tests are should be plain JUnit unit tests which are packaged to include
all their dependencies (fat/uber jar) and to be executable. This way we can
add the jar itself as a resource to a Jet job and execute any test we want without
 messing with the test environment classpath.

Continuous Delivery is set up for this project, so whenever a commit happens in
this repository, the build is triggered and if successfull resulting jars will
be uploaded to the [Amazon S3 Jet Test Jars](http://s3.amazonaws.com/jet-test-jars)
bucket.

## Building the Project

```
mvn clean package
```

The tests can be skipped if there are any third-party dependency (e.g. a running Kafka process)
which cannot be satisfied on compile time.

```
mvn clean package -DskipTests
``` 

