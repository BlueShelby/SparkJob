# syntax=docker/dockerfile:1

############################################
# Base image - common to all stages
############################################
FROM apache/spark-py:v3.4.0 as base

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYSPARK_MAJOR_PYTHON_VERSION=3

############################################
# Build image
############################################
FROM base as build

# Run as root for build tasks
USER 0

ENV APP_PATH="/app"
ENV VENV_PATH="$APP_PATH/.venv"
ENV PATH="$VENV_PATH/bin:$PATH"

ENV POETRY_VIRTUALENVS_IN_PROJECT=true
ENV POETRY_HOME="/opt/poetry"
ENV POETRY_NO_INTERACTION=1
ENV PATH="$POETRY_HOME/bin:$PATH"

WORKDIR $APP_PATH

RUN apt-get update && apt-get install --no-install-recommends -y build-essential

# Install poetry
ADD https://raw.githubusercontent.com/python-poetry/install.python-poetry.org/main/install-poetry.py install-poetry.py
RUN python3 install-poetry.py && poetry self add poetry-plugin-bundle

# Install dependencies
COPY poetry.lock pyproject.toml ./
RUN --mount=type=cache,target=/root/.cache/ poetry check && poetry install --no-root

# Bundle code and dependencies
COPY . .
RUN --mount=type=cache,target=/root/.cache/ poetry bundle venv --without=dev ./bundled
RUN poetry run venv-pack -p ./bundled/ -o pyspark_venv.tar.gz

############################################
# Test image
############################################
FROM build as test

RUN poetry run pytest

############################################
# Final image
############################################
FROM base as package

USER 0

# Add jars for S3 access
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/hadoop-aws-3.3.4.jar
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar  /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

WORKDIR /opt/spark/work-dir

COPY --from=build /app/pyspark_venv.tar.gz pyspark_venv.tar.gz
COPY run_job.py .

# default uid for spark image
#ARG spark_uid=185
#USER ${spark_uid}