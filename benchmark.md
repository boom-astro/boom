# Start Benchmark

## Setup

### Pull LFS files
```bash
  git lfs install
  git lfs pull
```

### Build Docker Image
```bash
  docker buildx create --use
  docker buildx inspect --bootstrap
  docker buildx bake -f tests/throughput/compose.yaml --load
```

### Download Data
```bash
  curl -s https://install.astral.sh | bash
  mkdir -p ./data/alerts
  curl -L "https://drive.google.com/uc?id=1BG46oLMbONXhIqiPrepSnhKim1xfiVbB" -o ./data/alerts/kowalski.NED.json.gz
  mkdir -p ./data/alerts/ztf/public/20250311
```

### Run Benchmark
```bash
  uv run tests/throughput/run.py
```
