FROM python:3.11-slim

WORKDIR /app

# Install system dependencies needed by psycopg2
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install dbt — consumer triggers dbt runs directly
RUN pip install --no-cache-dir dbt-postgres==1.7.9

# Copy consumer source code
COPY consumer/ .

# Copy dbt project into the container
COPY dbt_project ./dbt_project

CMD ["python", "kafka_consumer.py"]