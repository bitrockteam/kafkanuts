FROM maven:3.9-eclipse-temurin-26@sha256:b2c1ad85954592f9928e84327c65201f308ad9b5d8ed7d5b823717c97bf23fbb
WORKDIR /workspace
COPY pom.xml .
COPY modules modules
COPY config config
RUN mvn -B -ntp -DskipTests verify
