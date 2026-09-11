FROM gcr.io/dataflow-templates-base/python311-template-launcher-base:latest

ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/template/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/pipeline.py"

COPY pipeline.py /template/
COPY requirements.txt /template/

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /template/requirements.txt
