#!/bin/bash
# Configure Grafana dashboard provisioning based on environment
# In dev mode: dashboards are editable
# In prod mode: dashboards are read-only

set -euo pipefail

DASH_CONFIG="/etc/grafana/provisioning/dashboards/dashboards.yaml"

# Check if running in dev mode (default is false for production)
if [[ "${GRAFANA_DASHBOARD_EDITABLE:-false}" == "true" ]]; then
    EDITABLE="true"
else
    EDITABLE="false"
fi

# Create/update the dashboards provisioning config with environment-aware settings
write_dashboard_config() {
  cat > "$DASH_CONFIG" <<EOF
apiVersion: 1

providers:
  - name: boom
    orgId: 1
    folder: boom
    type: file
    disableDeletion: false
    updateIntervalSeconds: 30
    allowUiUpdates: ${EDITABLE}
    options:
      path: /var/lib/grafana/dashboards
EOF
}

if [[ -w "$DASH_CONFIG" ]] || [[ ! -e "$DASH_CONFIG" && -w "$(dirname "$DASH_CONFIG")" ]]; then
  write_dashboard_config
  echo "Dashboard provisioning configured with allowUiUpdates=${EDITABLE}"
else
  echo "Dashboard provisioning file is read-only; using checked-in config at ${DASH_CONFIG}"
fi

exec "$@"
