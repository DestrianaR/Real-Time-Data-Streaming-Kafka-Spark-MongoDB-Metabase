# Use the official OpenJDK 8 image as the base image
FROM openjdk:8

USER root

# Set the working directory inside the container
WORKDIR /usr/src/app

# Copy the application files into the container
COPY . .

# Add the Scala SBT repository to the sources list
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list

# Import the repository's public key to the system
RUN curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add

# Update the package list and install SBT
RUN apt-get update  && apt-get install sbt -y

# Grant execution permissions to entrypoint.sh
RUN chmod +x entrypoint.sh

# Set the entry point to entrypoint.sh
ENTRYPOINT [ "./entrypoint.sh" ]


