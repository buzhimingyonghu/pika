#!/bin/bash
# This script stops all Pika instances and removes generated files and directories.

# Define the list of configuration files and data directories
configs=(
  "./pika_single.conf"
  "./pika_master.conf"
  "./pika_slave.conf"
  "./pika_rename.conf"
  "./pika_acl_both_password.conf"
  "./pika_acl_only_admin_password.conf"
  "./pika_has_other_acl_user.conf"
)
data_dirs=(
  "./master_data"
  "./slave_data"
  "./rename_data"
  "./acl1_data"
  "./acl2_data"
  "./acl3_data"
)

# Stop all Pika processes
echo "Stopping Pika processes..."
pkill -f pika

# Wait a moment to ensure all processes are terminated
sleep 5

# Remove configuration files
echo "Removing configuration files..."
for config in "${configs[@]}"; do
  if [ -f "$config" ]; then
    rm -f "$config" "$config.bak"
    echo "Removed $config"
  fi
done

# Remove data directories
echo "Removing data directories..."
for dir in "${data_dirs[@]}"; do
  if [ -d "$dir" ]; then
    rm -rf "$dir"
    echo "Removed $dir"
  fi
done

echo "Cleanup complete."