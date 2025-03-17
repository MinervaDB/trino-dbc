#!/bin/bash
#
# Installation script for Trino ODBC Driver on Alpine Linux
#

set -e  # Exit on error

echo "===== Trino ODBC Driver Installer ====="
echo "This script will install the Trino ODBC Driver for Alpine Linux"
echo ""

# Check if running as root
if [ "$(id -u)" != "0" ]; then
   echo "This script must be run as root" 1>&2
   exit 1
fi

# Install dependencies
echo "Installing dependencies..."
apk update
apk add --no-cache \
    python3 \
    py3-pip \
    build-base \
    unixodbc \
    unixodbc-dev \
    curl \
    ca-certificates \
    bash \
    docker \
    docker-compose

# Create directories
echo "Creating directories..."
mkdir -p /opt/trino-odbc
mkdir -p /etc/odbc
mkdir -p /var/log/trino-odbc

# Copy files
echo "Copying files..."
cat > /opt/trino-odbc/trino_odbc_driver.py << 'EOF'
#!/usr/bin/env python3
"""
Trino ODBC Driver for Alpine Linux
Enables .NET applications to connect to Trino via ODBC
"""

# The full Python script content from trino_odbc_driver.py goes here
EOF

cat > /opt/trino-odbc/requirements.txt << 'EOF'
# Python dependencies for Trino ODBC Driver
flask==2.0.1
trino==0.315.0
pyodbc==4.0.32
gunicorn==20.1.0
requests==2.26.0
python-dotenv==0.19.0
EOF

cat > /opt/trino-odbc/docker-compose.yml << 'EOF'
version: '3'

services:
  trino-odbc:
    build:
      context: .
    ports:
      - "8991:8991"
    volumes:
      - /var/log/trino-odbc:/var/log
    restart: unless-stopped
EOF

cat > /opt/trino-odbc/Dockerfile << 'EOF'
FROM python:3.9-alpine

LABEL maintainer="Trino ODBC Driver"
LABEL version="1.0.0"
LABEL description="Python-based Trino ODBC Driver for Alpine Linux"

# Install system dependencies
RUN apk update && apk add --no-cache \
    build-base \
    unixodbc \
    unixodbc-dev \
    curl \
    ca-certificates \
    bash

# Create app directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY trino_odbc_driver.py .

# Create log directory
RUN mkdir -p /var/log

# Expose the API port
EXPOSE 8991

# Set environment variables for unixODBC
ENV ODBCSYSINI=/etc/odbc
ENV ODBCINI=/etc/odbc/odbc.ini
ENV ODBCSYSINSTINI=/etc/odbc/odbcinst.ini

# Create ODBC configuration directories
RUN mkdir -p /etc/odbc

# Create ODBC configuration files
RUN echo "[ODBC Drivers]\nTrino ODBC Driver = Installed\n\n[Trino ODBC Driver]\nDriver = /app/trino_odbc_driver.py\nSetup = /app/trino_odbc_driver.py\nDescription = Trino ODBC Driver for Alpine Linux\n" > /etc/odbc/odbcinst.ini
RUN echo "[ODBC Data Sources]\nTrino = Trino ODBC Driver\n\n[Trino]\nDriver = Trino ODBC Driver\nDescription = Trino ODBC Driver for Alpine Linux\nServer = localhost\nPort = 8991\n" > /etc/odbc/odbc.ini

# Set entrypoint
ENTRYPOINT ["python", "trino_odbc_driver.py"]

# Default command
CMD ["--host", "0.0.0.0", "--port", "8991"]
EOF

# Make the driver executable
chmod +x /opt/trino-odbc/trino_odbc_driver.py

# Configure ODBC
echo "Configuring ODBC..."
cat > /etc/odbc/odbcinst.ini << 'EOF'
[ODBC Drivers]
Trino ODBC Driver = Installed

[Trino ODBC Driver]
Driver = /opt/trino-odbc/trino_odbc_driver.py
Setup = /opt/trino-odbc/trino_odbc_driver.py
Description = Trino ODBC Driver for Alpine Linux
EOF

cat > /etc/odbc/odbc.ini << 'EOF'
[ODBC Data Sources]
Trino = Trino ODBC Driver

[Trino]
Driver = Trino ODBC Driver
Description = Trino ODBC Driver for Alpine Linux
Server = localhost
Port = 8991
EOF

# Create systemd service file
echo "Creating service file..."
cat > /etc/init.d/trino-odbc << 'EOF'
#!/sbin/openrc-run

name="trino-odbc"
description="Trino ODBC Driver Service"
command="/usr/bin/docker-compose"
command_args="-f /opt/trino-odbc/docker-compose.yml up -d"
command_background="true"
pidfile="/run/${RC_SVCNAME}.pid"
output_log="/var/log/trino-odbc/service.log"
error_log="/var/log/trino-odbc/service.err"

depend() {
    need net
    after firewall
}

stop() {
    ebegin "Stopping ${name}"
    /usr/bin/docker-compose -f /opt/trino-odbc/docker-compose.yml down
    eend $?
}
EOF

chmod +x /etc/init.d/trino-odbc
rc-update add trino-odbc default

# Build and start the service
echo "Building Docker image..."
cd /opt/trino-odbc
docker-compose build

echo "Starting Trino ODBC Driver service..."
docker-compose up -d

echo ""
echo "===== Installation Complete ====="
echo "Trino ODBC Driver has been installed and started."
echo "Service is running at http://localhost:8991"
echo ""
echo "To check status:"
echo "  docker-compose -f /opt/trino-odbc/docker-compose.yml ps"
echo ""
echo "To view logs:"
echo "  docker-compose -f /opt/trino-odbc/docker-compose.yml logs"
echo ""
echo "To stop the service:"
echo "  docker-compose -f /opt/trino-odbc/docker-compose.yml down"
echo ""
echo "To start the service:"
echo "  docker-compose -f /opt/trino-odbc/docker-compose.yml up -d"
echo ""
