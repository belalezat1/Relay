FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /workspace

COPY pom.xml ./
COPY core/pom.xml core/pom.xml
COPY api/pom.xml api/pom.xml
COPY core/src core/src
COPY api/src api/src

RUN mvn -q -pl api -am clean package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/api/target/relay-api-*.jar /app/app.jar

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
