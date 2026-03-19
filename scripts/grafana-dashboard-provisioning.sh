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

echo "Dashboard provisioning configured with allowUiUpdates=${EDITABLE}"
exec "$@"
