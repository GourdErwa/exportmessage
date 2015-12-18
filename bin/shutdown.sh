#!/usr/bin/env bash
kill -15 $(ps -ef| grep ezsonar.ExportMessage | awk '{print $2}')
