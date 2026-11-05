FROM python:3.12-slim
WORKDIR /app
COPY requirements-common.txt /app/requirements-common.txt
COPY audio_service/requirements.txt /app/audio_service/requirements.txt
RUN pip install --no-cache-dir -r /app/audio_service/requirements.txt
COPY common /app/common
COPY audio_service /app/audio_service
ENV PYTHONPATH=/app
CMD ["python", "-m", "audio_service.main"]
