import boto3
import os.path
import glob
import datetime

from avro.datafile import DataFileReader
from avro.io import DatumReader

FILES_TO_FETCH = 15000

def select_distance_accumulated(car):
  return car['distanceAccumulated']

def select_timestamp(car):
  return car['timestamp']


def select_reg_number(car):
  return car['regNumber']

def get_carmap_bucket():
  session = boto3.Session(profile_name='carmap')
  s3 = session.resource('s3')
  return s3.Bucket("carmap")


def cache_path(key):
  return "cache/{0}".format(key)

def download_file(bucket, key):
  destination_path = cache_path(key)
  if not os.path.isfile(destination_path):
    bucket.download_file(key, destination_path)

def get_files_for_provider(provider):
  return glob.glob("cache/*.{0}.avro".format(provider))

def print_progress(idx, total):
  if idx%100 == 0:
    print "Processed {0} out of {1} files".format(idx, total)


def build_cars_in_time(files):
  cars = {}

  for idx, file in enumerate(files):
    print_progress(idx, len(files))

    try:
      reader = DataFileReader(open(file, "rb"), DatumReader())
      for car in reader:
        car_reg_number = select_reg_number(car)
        if not cars.has_key(car_reg_number):
            cars[car_reg_number] = []
        cars[car_reg_number].append({
          'timestamp': select_timestamp(car),
          'distanceAccumulated': select_distance_accumulated(car),
          'regNumber': select_reg_number(car)
        })
    except TypeError:
      print("Error reading file {0}".format(file))
    finally:
      reader.close()

  return cars

def parse_timestamp(timestamp):
  return datetime.datetime.fromtimestamp(timestamp/1000.0).strftime('%Y-%m-%d %H:%M:%S')

def get_date(timestamp):
  return datetime.datetime.fromtimestamp(timestamp/1000.0).strftime('%Y-%m-%d')

def display_car_disntance(car):
  print "{0}: {1}km".format(parse_timestamp(select_timestamp(car)), select_distance_accumulated(car))

def sorted_by_timestamp(car_in_time):
  return sorted(car_in_time, key=lambda car: select_timestamp(car))

def show_progress(car_in_time):
  print("Showing progress for: {0}".format(select_reg_number(car_in_time[0])))

  last_distance = None
  for car in car_in_time:
    if select_distance_accumulated(car) != last_distance:
      display_car_disntance(car)
      last_distance = select_distance_accumulated(car)

def get_daily_distances(car_in_time):
    distances = []
    first_snapshot = car_in_time[0]
    last_date = get_date(select_timestamp(first_snapshot))
    last_distance = select_distance_accumulated(first_snapshot)
    for car in car_in_time:
        if get_date(select_timestamp(car)) != last_date:
            distances.append(select_distance_accumulated(car) - last_distance)
            last_date = get_date(select_timestamp(car))
            last_distance = select_distance_accumulated(car)
    return distances

def calculate_average(l):
  if len(l) == 0:
    return 0
  return reduce(lambda x, y: x + y, l) / float(len(l))

bucket = get_carmap_bucket()

print("Fetching files...")
for idx, obj in enumerate(bucket.objects.limit(count=FILES_TO_FETCH)):
  print_progress(idx, FILES_TO_FETCH)
  download_file(bucket, obj.key)
files = get_files_for_provider("traficar")
print("Files fetched!")

print("Building cars...")
cars_in_time = build_cars_in_time(files)
print("Cars ready!")

all_distances = []
for key in cars_in_time.keys():
  car = sorted_by_timestamp(cars_in_time[key])
  show_progress(car)
  average_daily = calculate_average(get_daily_distances(car))
  all_distances.append(average_daily)
  print(get_daily_distances(car))
  print("Average daily: {0}km".format(average_daily))
print("All average {0}".format(calculate_average(all_distances)))