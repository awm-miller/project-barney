#!/usr/bin/env python3

import os
import sys
import subprocess

# Channel ID
channel_id = "UC_fNP2ettM5ius4BZ6G7MOg"

# Read query from file
with open('query.txt', 'r', encoding='utf-8') as f:
    query = f.read().strip()

# Build command
cmd = [
    'python', 
    'search_channel_videos.py', 
    '--channels', 
    channel_id,
    '--title-query', 
    query
]

# Execute the script
result = subprocess.run(cmd, capture_output=True, text=True)

# Print output
print("STDOUT:")
print(result.stdout)

print("\nSTDERR:")
print(result.stderr)

print(f"\nExit code: {result.returncode}") 