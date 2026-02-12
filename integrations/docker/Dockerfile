FROM python:3.12-slim

WORKDIR /app
COPY . /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        wget \
    && rm -rf /var/lib/apt/lists/*

RUN wget -q https://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz \
    && tar -xzf ta-lib-0.4.0-src.tar.gz \
    && cd ta-lib \
    && ./configure --prefix=/usr \
    && make -j\"$(nproc)\" \
    && make install \
    && cd /app \
    && rm -rf ta-lib ta-lib-0.4.0-src.tar.gz

RUN pip install --upgrade pip \
    && pip install pytest \
    && pip install .

CMD ["pytest", "-q"]
