# Use official Python runtime
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN python3 -m spacy download en_core_web_sm
RUN python3 -m spacy download en_core_web_lg

# Copy all project files
COPY . .

# Expose port
EXPOSE 8000

# Run with Uvicorn
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
