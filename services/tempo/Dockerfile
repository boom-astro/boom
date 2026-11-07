FROM python:3.12-slim

WORKDIR /app

# CPU-only wheel: one object per request does not need a GPU, and the default
# PyPI wheel bundles CUDA libraries several GB in size.
RUN pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu "torch>=2.9,<3"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pinned to a commit so a push upstream cannot change the preprocessing under a
# deployed model. Bump it together with any bundle trained on newer code.
ARG TEMPO_HYRAX_REF=509931b05b3678fac7d712a9f78ed3440f08cfc5
RUN pip install --no-cache-dir --no-deps \
    "https://github.com/frenbox/hyrax_tempo/archive/${TEMPO_HYRAX_REF}.tar.gz"

COPY app.py predictor.py ./

USER nobody
EXPOSE 8500
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8500"]
