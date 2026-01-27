FROM python:3.11-slim

# System deps for LibreOffice PDF conversion
RUN apt-get update && apt-get install -y --no-install-recommends \
    libreoffice \
    libreoffice-calc \
    fonts-dejavu \
    fontconfig \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install python deps
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the project
COPY . /app

# Railway sets PORT automatically
ENV PORT=8000

CMD ["bash", "-lc", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT}"]
