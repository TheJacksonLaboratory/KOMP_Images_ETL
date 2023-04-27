FROM python:3

# Copy the contents of the repo into the /app folder inside the container
COPY . /app

# Update the current working directory to the /app folder
WORKDIR /app
RUN dir

# Install the dependencies of the tool
RUN pip install -r requirements.txt

# Provide an entry point for the project
ENTRYPOINT [ "python", "src/App.py" ]

CMD ["python","src/App.py"]






