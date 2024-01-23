#!/usr/bin/python3

import sys
import csv

def medication_count_reducer():
    csv_writer = csv.writer(sys.stdout, delimiter=',')
    medication_counts = {}

    for line in sys.stdin:
        # Parse the input
        medication, count = line.strip().split("\t")

        # Convert count to integer
        count = int(count)

        # Update the count for the medication in the dictionary
        medication_counts[medication] = medication_counts.get(medication, 0) + count
    print(f'{medication_counts}')

    # # Emit the final results
    # for medication, count in medication_counts.items():
    #     csv_writer.writerow()

if __name__ == "__main__":
    medication_count_reducer()
    sys.stdout.flush()
