#!/bin/bash

if ! which psql &> /dev/null 2>&1; then
    echo "PostgreSQL is not installed. Installing..."
    sudo apt update
    sudo apt install postgresql -y
    sudo systemctl start postgresql
    sudo systemctl enable postgresql
fi


# Variables
username="amaach"
password="mysecretpassword"
database="piscineds"
# Create user with password
sudo -u postgres psql -c "CREATE ROLE $username WITH LOGIN PASSWORD '$password';"

# Create database owned by the new user
sudo -u postgres psql -c "CREATE DATABASE $database WITH OWNER = $username;"
# Grant privileges
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE $database TO $username;"

echo "Setup complete."