## Serverless Architecture with Java 8, AWS Lambda

This is supposed to be very lightweight jar in terms of size.
Because, Lambdas are charged by amount of memoery that is configured to run each function.
And I noticed that, lambda performance is directly proportional to amount of memoery we configure.

To reduce the resultant shaded jar file, I have copied some of the files from le-main project. 
And I explicitely avoided the dependencies from current project as it pulls in lot of transitive dependencies.

Ex: I have copied the domain class: GatewayPolicyConfiguration.class, instead of including le-domain as dependency.