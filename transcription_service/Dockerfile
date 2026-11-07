FROM python:3.12-slim
ARG INSTALL_LOCAL_AI=false
WORKDIR /app
COPY requirements-common.txt /app/requirements-common.txt
COPY transcription_service/requirements.txt /app/transcription_service/requirements.txt
COPY transcription_service/requirements-ai.txt /app/transcription_service/requirements-ai.txt
RUN pip install --no-cache-dir -r /app/transcription_service/requirements.txt && \
    if [ "$INSTALL_LOCAL_AI" = "true" ]; then pip install --no-cache-dir -r /app/transcription_service/requirements-ai.txt; fi
COPY common /app/common
COPY transcription_service /app/transcription_service
ENV PYTHONPATH=/app
CMD ["python", "-m", "transcription_service.main"]
