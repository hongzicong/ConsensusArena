#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C

run_dir=$1
expected_repetitions=$2
summary="$run_dir/summary.csv"
details="$run_dir/repetition-summaries.csv"
operations=(READ UPDATE ALL)

shopt -s nullglob
files=("$run_dir"/repetition-*/results/summary.csv)
if [[ ${#files[@]} != "$expected_repetitions" ]]; then
    echo "Expected $expected_repetitions repetition summaries but found ${#files[@]}" >&2
    exit 1
fi
mapfile -t regions < <(awk -F, 'NR > 1 && !seen[$1]++ { print $1 }' "${files[0]}")

printf 'Repetition,Region,Operation,Count,MeanMs,MedianMs,P95Ms,P99Ms,MinMs,MaxMs,FallbackRatio\n' > "$details"
for file in "${files[@]}"; do
    repetition=$(basename "$(dirname "$(dirname "$file")")")
    awk -F, -v repetition="$repetition" 'NR > 1 { print repetition "," $0 }' "$file" >> "$details"
done

printf 'Region,Operation,Repetitions,TotalSamples,MeanMsAvg,MeanMsStdDev,MedianMsAvg,MedianMsStdDev,P95MsAvg,P95MsStdDev,P99MsAvg,P99MsStdDev,MinMsAvg,MinMsStdDev,MaxMsAvg,MaxMsStdDev,FallbackRatioAvg,FallbackRatioStdDev\n' > "$summary"
for region in "${regions[@]}"; do
    for operation in "${operations[@]}"; do
        awk -F, -v region="$region" -v operation="$operation" -v expected="$expected_repetitions" '
            $1 == region && $2 == operation {
                repetitions++
                samples += $3
                for (column = 4; column <= 9; column++) {
                    sum[column] += $column
                    sumsq[column] += $column * $column
                }
                if ($10 != "NA" && $10 != "") {
                    fallbackRepetitions++
                    fallbackSum += $10
                    fallbackSumSq += $10 * $10
                }
            }
            END {
                if (repetitions == 0) {
                    exit 0
                }
                if (repetitions != expected) {
                    printf "Expected %d rows for %s/%s but found %d\n", expected, region, operation, repetitions > "/dev/stderr"
                    exit 1
                }
                if (samples == 0) {
                    printf "%s,%s,%d,0", region, operation, repetitions
                    for (column = 4; column <= 9; column++) printf ",,"
                    printf ",NA,NA\n"
                    exit 0
                }
                if (fallbackRepetitions != 0 && fallbackRepetitions != expected) {
                    printf "Expected fallback ratios for all or none of %s/%s repetitions but found %d/%d\n", region, operation, fallbackRepetitions, expected > "/dev/stderr"
                    exit 1
                }
                printf "%s,%s,%d,%d", region, operation, repetitions, samples
                for (column = 4; column <= 9; column++) {
                    average = sum[column] / repetitions
                    variance = repetitions > 1 ? (sumsq[column] - sum[column] * sum[column] / repetitions) / (repetitions - 1) : 0
                    if (variance < 0 && variance > -1e-9) variance = 0
                    printf ",%.3f,%.3f", average, sqrt(variance)
                }
                if (fallbackRepetitions == 0) {
                    printf ",NA,NA"
                } else {
                    fallbackAverage = fallbackSum / fallbackRepetitions
                    fallbackVariance = fallbackRepetitions > 1 ? (fallbackSumSq - fallbackSum * fallbackSum / fallbackRepetitions) / (fallbackRepetitions - 1) : 0
                    if (fallbackVariance < 0 && fallbackVariance > -1e-15) fallbackVariance = 0
                    printf ",%.6f,%.6f", fallbackAverage, sqrt(fallbackVariance)
                }
                printf "\n"
            }
        ' "${files[@]}" >> "$summary"
    done
done

cat "$summary"
printf 'Repetition details: %s\n' "$details"
printf 'Repetition summary: %s\n' "$summary"
