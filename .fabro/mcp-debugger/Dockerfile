FROM node:22-bookworm-slim AS node
FROM python:3.13-slim-bookworm
COPY --from=node /usr/local/bin/node /usr/local/bin/node
COPY --from=node /usr/local/lib/node_modules /usr/local/lib/node_modules
RUN ln -s ../lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm \
    && ln -s ../lib/node_modules/npm/bin/npx-cli.js /usr/local/bin/npx \
    && apt-get update && apt-get install -y --no-install-recommends \
       bash util-linux git ca-certificates curl gcc libpq-dev libcairo2 libffi-dev \
       libgdk-pixbuf-2.0-0 libpango-1.0-0 libpangocairo-1.0-0 libxml2 \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir uv==0.12.10 debugpy==1.8.16 \
    && npm install --global --no-audit --no-fund bun@1.3.14 \
       @oh-my-pi/pi-coding-agent@18.1.11 @automatalabs/codex-acp@2.4.1 @openai/codex@0.153.4
WORKDIR /opt/fabro-debugger
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts --no-audit --no-fund
COPY gateway.mjs ./
WORKDIR /workspace
ENV NO_BROWSER=1
CMD ["sleep", "infinity"]
