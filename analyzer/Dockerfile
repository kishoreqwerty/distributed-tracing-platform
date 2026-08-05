FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir .
EXPOSE 9464
# 8000: the query API (analyzer.api.app), a separate process from the
# ENTRYPOINT below — deploy/docker-compose.yml's analyzer-api service
# overrides the entrypoint to run it instead. Same image, same installed
# package, two different processes.
EXPOSE 8000
ENTRYPOINT ["python", "-m", "analyzer.main"]
