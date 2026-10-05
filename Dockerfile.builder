FROM maven:3-eclipse-temurin-26@sha256:5bfb178bea156ab53b779b2be338e8bfb45decfed35d83da92f1dd9b7348845c
WORKDIR /workspace
COPY pom.xml .
COPY modules modules
COPY config config
RUN mvn -B -ntp -DskipTests verify
