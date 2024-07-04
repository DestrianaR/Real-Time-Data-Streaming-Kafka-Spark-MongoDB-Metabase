# Gunakan gambar dasar dari Spark
FROM bitnami/spark:latest

USER root

# Install sbt
RUN mkdir -p /var/lib/apt/lists/partial && \
    apt-get update && \
    apt-get install -y curl gnupg && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x99E82A75642AC823" | apt-key add && \
    apt-get update && \
    apt-get install -y sbt

# Salin file aplikasi ke dalam container
WORKDIR /app
COPY . .

# Jalankan sbt untuk membangun aplikasi
RUN sbt clean assembly



