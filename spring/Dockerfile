FROM maven:3.3-jdk-8-alpine
MAINTAINER salquier

COPY pom.xml /app/
COPY src /app/

RUN cd /app && \
    mvn package -DskipTests -q -B && \
	cd .. && \
	mv /app/target/myhealthpartnerbackend-1.0-SNAPSHOT.jar app.jar && \
	rm -rf /app && \
	rm -rf /root/.m2
	
EXPOSE 8080
ENTRYPOINT java -jar app.jar > /var/log/spring.log 2> /var/log/spring.error.log
