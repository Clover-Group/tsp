#!/usr/bin/env bash

REPORTS_DIR="${1}"

if [[ $# -lt 1 ]]; then
    echo >&2 "You have to provide the directory containing your coverage reports."
    exit 2
fi

total_coverage=0
count=0

for i in $(find "$REPORTS_DIR" -name '*.xml');
do
    printf "Found coverage report: %s\n" "$i"
    line_rate="$(head -n 4 ${i} | sed 'N;s/.*line-rate="\([^" ]*\).*/\1/g')"
    coverage=$(echo "${line_rate} * 100" | bc)
    printf "PARTIAL_COVERAGE: %2.2f\n" "$coverage"
    count=$((count + 1))
    total_coverage=$(echo "${total_coverage} + ${coverage}" | bc)
done;

printf "Found a total of %i report(s)\n" "$count"
printf "TOTAL_COVERAGE=%2.2f\n" "$(echo "${total_coverage} / ${count}" | bc -l)"
