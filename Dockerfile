FROM python:3.11-slim

# Set work directory
WORKDIR /app

# Copy files
COPY api/ /app/

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Expose port
EXPOSE 8000

# Run FastAPI app using uvicorn
CMD ["uvicorn", "hot-service:app", "--host", "0.0.0.0", "--port", "8000"]
