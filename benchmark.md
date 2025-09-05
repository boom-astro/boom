# Running Benchmark

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
  # Also needed for Apptainer (To build the SIF file with the image)
```

### Download Data
```bash
  mkdir -p ./data/alerts/ztf/public/20250311
  gdown "https://drive.google.com/uc?id=1BG46oLMbONXhIqiPrepSnhKim1xfiVbB" -O ./data/alerts/kowalski.NED.json.gz
```

### Run Benchmark With Docker
```bash
  uv run tests/throughput/run.py
```

### Run Benchmark With Apptainer
```bash
  ./apptainer/def/build-sif.sh # Build the SIF file
  python apptainer/run.py # Run the benchmark
```
