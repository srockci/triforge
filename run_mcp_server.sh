#!/bin/bash
cd /root/openmanus-integration && exec /root/openmanus-integration/.venv/bin/python -m openmanus_server.mcp_server "$@"
