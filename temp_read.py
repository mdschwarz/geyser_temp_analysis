import csv
import argparse
import time
from datetime import datetime
import sys


class StateMachine:

    def __init__(self):
        self.process = self.process_begin

        self.running_temp = None
        self.eruption_start = None
        self.eruption_end = None
        self.last_peak = None
        self.offcounter = 0
        self.down_counter = 0
   
    def increment_temp(self, temp):
        self.running_temp = (self.running_temp + temp) / 2

    def process_begin(self, time, temp):
        self.running_temp = temp
        self.process = self.process_non_erupting
	return False

    def process_non_erupting(self, time, temp):
        if temp - self.running_temp > 30:
            self.last_peak = temp
            self.process = self.process_erupting
            self.eruption_start = time
            self.down_counter = 0

        if temp < self.running_temp - 10:
            # Anomaly.  Ignore.
            return False

        self.running_temp = temp
	return False

    def process_erupting(self, time, temp):
        # Filter garbage
	if temp < self.running_temp - 20:
            return False
        if temp < self.running_temp:
            if self.down_counter == 0:
                self.eruption_end = time
            self.down_counter += 1
        else:
            self.down_counter = 0

        self.increment_temp(temp)

        if temp < self.last_peak - 20:
            if self.offcounter >= 3:
                self.process = self.process_non_erupting
                return True
            else: self.offcounter += 1
        else:
            self.offcounter = 0

        return False

    def get_eruption(self):
        return (self.eruption_start, self.eruption_end)


def get_next_temp(temp_file):
    """generator to read one line of data at a time"""
    reader = csv.reader(temp_file)
    reader.next() # skip header row
    for row in reader:
        if not row[0]:
            break
        if not int(row[0]):
            continue
	(date, time, temperature) = (row[1], row[2], row[3])
	(day, month, year) = date.split('-')
	(hour, minute, second) = time.split(':')
        yield (datetime(int('20' + year), int(month), int(day), int(hour), int(minute), int(second)), float(temperature))


def process_data(infile, outfile):
    sm = StateMachine()
    time_writer = csv.writer(outfile)
    last_start = None
    serial_number = 1
    for (time, temp) in get_next_temp(infile):
        if temp < 0:
            # Filter some garbage
            continue
        if sm.process(time, temp):
            (start, end) = sm.get_eruption()
            duration = end - start
            if last_start:
                interval = start - last_start
            else:
                interval = None
            last_start = start
            time_writer.writerow([serial_number, start, end, duration, interval])
            print "%d, %s, %s, %s, %s" % (serial_number, start, end, duration, interval)
            serial_number += 1


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("temp_file", help="a csv temperature data file")
    parser.add_argument("out_file", help="a csv eruption record file")
    return parser.parse_args()


def main():
    args = get_args()
    with open(args.temp_file, 'r') as tempfile, open(args.out_file, 'w') as outfile:
        process_data(tempfile, outfile)


if __name__ == "__main__":
    sys.exit(main())
