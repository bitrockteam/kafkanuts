FROM maven:3.9.11-eclipse-temurin-21@sha256:463a1849665463254b2dd56e3a5b316f1596bc93d0571065c06ea05bb48ab8f4
WORKDIR /workspace
COPY pom.xml .
COPY modules modules
COPY config config
RUN mvn -B -ntp -DskipTests verify
