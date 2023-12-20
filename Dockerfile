# Use the official Python image
FROM python:3.9

# Set the working directory
WORKDIR /app

# Copy the requirements file to the working directory
COPY requirements.txt .

# Install any dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container at /app
COPY . /app

# Set environment variables
ENV PROJECT_ID vadimzaripov-477-2022062208552
ENV DATASET_ID britain_statistics
ENV TABLE_NAME boe_baserate

# Expose the port on which the app will run
EXPOSE 8080

# Run the application
CMD ["python", "main.py"]
