# Use Python 3.12 as the base image
FROM python:3.12-slim

# Set the working directory inside the container
WORKDIR /app/scrapy-unified-scraper

# Copy the whole scrapy-unified-scraper directory to the container
COPY scrapy-unified-scraper /app/scrapy-unified-scraper

# Install necessary dependencies (including Scrapy, AWS CLI, and jq)
RUN apt-get update && \
    apt-get install -y jq && \
    pip install --upgrade pip && \
    pip install -r /app/scrapy-unified-scraper/requirements.txt && \
    pip install awscli

# Expose the port that Scrapy will use (default is 6800)
EXPOSE 6800
