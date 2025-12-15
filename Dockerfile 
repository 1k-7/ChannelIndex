FROM python:3.11-slim

# Set the working directory
WORKDIR /app

# Copy all files from the current directory to the container
COPY . /app

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Command to run the bot
CMD ["python", "main_bot.py"]