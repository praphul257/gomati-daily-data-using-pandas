# Step 1: Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the timezone
ENV TZ=Asia/Kolkata

# Step 2: Set the working directory in the container
WORKDIR /app

# Step 3: Copy the current directory contents into the container at /app
COPY . /app

# Step 4: Install any necessary dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Step 5: Load environment variables from .env
COPY . .

# # Step 6: Run the application
CMD ["python", "main.py"]

# # Step 6: Start Celery worker and schedule with Beat
# CMD ["celery", "-A", "tasks", "worker", "--loglevel=info"]
