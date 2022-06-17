FROM python:3.8-slim

RUN pip install --upgrade pip
RUN pip install poetry

WORKDIR /usr/local/nhl_job/

COPY poetry.lock pyproject.toml ./

RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi

COPY . /usr/local/nhl_job

ENTRYPOINT ["python", "-m", "nhldata.app"]