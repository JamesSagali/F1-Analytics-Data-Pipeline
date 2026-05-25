# Use the official Python image as the base image
FROM apache/airflow:2.9.1

# Set the working directory in the container
WORKDIR /app 

# Set the working directory in the container/ Copy the requirements.txt file to the container
COPY requirements.txt .

# Install the dependencies specified in the requirements.txt file
RUN pip install --no-cache-dir -r requirements.txt 

# Set the working directory in the container/ Copy the rest of the application code to the container
COPY . . /app 

EXPOSE 8888

# Command to start Jupyter Lab when the container is run, allowing access from any IP address and running on port 8888
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"] 