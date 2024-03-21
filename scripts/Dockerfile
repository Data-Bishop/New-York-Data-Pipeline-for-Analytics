FROM python:3.11.2

# Set the working directorry in the container
WORKDIR /app

# Copy the current directory into the conatiner at /app
COPY . /app

# Install any dependencies listed in the requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Run the python script when the container launches
CMD [ "python", "transform_and_ingest_data.py" ]