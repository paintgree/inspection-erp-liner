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

# Start app (NO shell variable expansion needed)
CMD ["python", "-c", "import os,uvicorn,re; p=os.environ.get('PORT','8000'); m=re.search(r'\\d+', str(p)); port=int(m.group(0)) if m else 8000; uvicorn.run('app.main:app', host='0.0.0.0', port=port)"]

