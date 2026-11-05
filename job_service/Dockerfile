FROM python:3.12-slim
WORKDIR /app
COPY requirements-common.txt /app/requirements-common.txt
COPY job_service/requirements.txt /app/job_service/requirements.txt
RUN pip install --no-cache-dir -r /app/job_service/requirements.txt
COPY common /app/common
COPY job_service /app/job_service
ENV PYTHONPATH=/app
CMD ["uvicorn", "job_service.main:app", "--host", "0.0.0.0", "--port", "8000"]
