FROM python:3.13.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/      
# we want to have uv in our docker container
# so we copied it from the docker image which already had uv in it and store it in uv/bin folder

WORKDIR /code
ENV PATH="/code/.venv/bin:$PATH"

# RUN pip install pandas pyarrow
COPY pyproject.toml .python-version uv.lock ./
RUN uv sync --locked       
# it now points to the right Python from our venv, and does not use python from our host system
# this ensures the dependencies in our docker are same as our host machine

COPY ingest_data.py .

ENTRYPOINT ["python", "ingest_data.py" ]        
# tells Docker,whenever someone runs a container from this image, always start by running python pipeline.py.








