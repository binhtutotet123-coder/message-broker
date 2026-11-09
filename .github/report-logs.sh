#!/usr/bin/env bash
# Emit the last lines of every *.log file as a GitHub error annotation, so a failed CI
# run can be diagnosed from the PR's Checks tab without opening the full job log.
for f in *.log; do
  [ -f "$f" ] || continue
  body=$(tail -n 40 "$f" | sed -e 's/%/%25/g' -e 's/\r/%0D/g' | sed -e ':a' -e 'N' -e '$!ba' -e 's/\n/%0A/g')
  echo "::error title=${f} (40 dòng cuối)::${body}"
done
