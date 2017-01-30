FROM maven:3.3-jdk-8-alpine
MAINTAINER salquier

RUN apk add --no-cache git \ 
   && git clone -b spring https://github.com/Serli/my-health-partner-backend.git \
   && cd my-health-partner-backend \
   && mvn package -DskipTests \
   && cd .. \
   && mv my-health-partner-backend/target/myhealthpartnerbackend-1.0-SNAPSHOT.jar app.jar \
   && rm -rf my-health-partner-backend \
   && apk del git

EXPOSE 8080
ENTRYPOINT java -jar app.jar > /var/log/spring.log 2> /var/log/spring.error.log
