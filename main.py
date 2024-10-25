import os
import copy
from dotenv import load_dotenv
load_dotenv()
from typing import Dict, Literal
from datetime import datetime
from pathlib import Path
import requests
import schedule
from threading import Thread, Lock
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import re
import time
import shutil
import json
import logging

class JobState:
  def __init__(self):
    self.job_id = None
    self.job_status = 'pending'
    self.job_step = 'init'

  def set_job_id(self, job_id: str):
    self.job_id = job_id

  def set_job_status(self, job_status: Literal['pending', 'in_progress', 'completed']):
    self.job_status = job_status

  def set_job_step(self, job_step: Literal['init', 'uploading', 'processing', 'saving', 'done']):
    self.job_step = job_step


class JobsGlobalState:
  def __init__(self):
    self.__json_file = os.getcwd() + '/jobs.json' 
    self.jobs: Dict[str, JobState] = {}

  def init_job_id(self, job_file: str):
    self.jobs[job_file] = JobState()
    
  def save_state(self):
    jobs_state = {}
    for job_file, job in self.jobs.items():
      job_obj_state = copy.copy(job.__dict__)
      jobs_state[job_file] = job_obj_state
    with open(self.__json_file, 'w') as f:
      json.dump(jobs_state, f)

  def load_state(self):
    if os.path.exists(self.__json_file):
      with open(self.__json_file, 'r') as f:
        jobs_state_json = json.load(f)
        for job_file, job in jobs_state_json.items():
          self.jobs[job_file] = JobState()
          self.jobs[job_file].set_job_id(job['job_id'])
          self.jobs[job_file].set_job_status(job['job_status'])
          self.jobs[job_file].set_job_step(job['job_step'])

  def get_job(self, job_id: str):
    return self.jobs[job_id]

  def deallocate_job(self, job_file: str):
    del self.jobs[job_file]

lock = Lock()

# constants

files_tail = []

threads: Dict[str, Thread] = {}

jobs_state = JobsGlobalState()

ALLOWED_FILES = ['.mp4', '.mp3', '.wav', '.ogg', '.m4a', '.mkv', '.opus', '.txt', '.md']

CWD = os.getcwd()

# BASE_URL = "https://mmonk-8893350593.us-central1.run.app/v1"

BASE_URL = "http://localhost:8080/v1"

API_KEY = os.getenv("SGAI_API_KEY")

INPUT_FILES = os.getenv("INPUT_FILES")

OUTPUT_FILES = os.getenv("OUTPUT_FILES")

OUTPUT_TEXT_FILES = os.getenv("OUTPUT_TEXT_FILES")

FILES_UPLOAD_MAX_QUANTITY = os.getenv("FILES_UPLOAD_MAX_QUANTITY", 1)

MAX_ALIVE_THREADS = os.getenv("MAX_ALIVE_THREADS", 2)

logging.basicConfig(
  filename=f'{CWD}/activity.log',
  level=logging.INFO,
  format='%(asctime)s - %(levelname)s - %(message)s'
)

uploading_files = 0

threads_alive = 0

if API_KEY == None:
  print("SGAI_API_KEY not found")
  exit(1)

if INPUT_FILES == None:
  print("INPUT_FILES not found")
  exit(1)

if OUTPUT_FILES == None:
  print("OUTPUT_FILES not found")
  exit(1)

if OUTPUT_TEXT_FILES == None:
  print("OUTPUT_FILES_TEXT not found")
  exit(1)

if FILES_UPLOAD_MAX_QUANTITY < 1:
  print(f"FILES_UPLOAD_MAX_QUANTITY must be greater than 0")
  exit(1)

if MAX_ALIVE_THREADS < 1:
  print(f"MAX_ALIVE_THREADS must be greater than 0")
  exit(1)

BLOB_SIZE = 31457280 # 30 MB

HEADERS = {
  "Authorization": "Bearer " + API_KEY
}

# funciones pricipales para API

def get_me_api():
  response = requests.get(BASE_URL + "/auth/users/me", headers=HEADERS)
  if response.status_code == 200:
    return response.json()
  print(response.content.decode("utf-8"))
  return None

def get_job_api(job_id: str):
  response = requests.get(BASE_URL + "/video_ia/jobs/" + job_id, headers=HEADERS)

  if response.status_code == 200:
    return response.json()
  print(f'error injob api')
  return None

def get_job_results_api(job_id: str, key: str):
  response = requests.get(BASE_URL + "/video_ia/jobs-results/" + job_id + "/" + key, headers=HEADERS)
  if response.status_code == 200:
    return response.content.decode('utf-8')
  return None

def upload_file_api(file_path: Path, project_name: str):
  with open(file_path, 'rb') as file:
    response = requests.post(BASE_URL + "/video_ia/process_file/" + project_name, headers=HEADERS, files={ 'file': (file_path.name, file.read())})
    if response.status_code == 200:
      return response.json()
    print(f'error in file api {response.content.decode("utf-8")} status code: {response.status_code}')
    return None

def initialize_file_upload_api(file_name: str, project_name: str, size: int):
  response = requests.post(BASE_URL + "/video_ia/file/initialize/" + project_name + "/" + file_name + "?size=" + str(size), headers=HEADERS)
  if response.status_code == 200 or response.status_code == 409:
    return response.json()
  print(f'error in file init api {response.content.decode("utf-8")} status code: {response.status_code}')
    
  return None

def upload_file_blob_api(file_path: Path, project_name: str, position: int):
  with open(file_path, 'rb') as file:
    file.seek(position)
    blob = file.read(BLOB_SIZE)
    response = requests.post(BASE_URL + "/video_ia/file/write_blob/" + project_name + "/" + file_path.name + "/" + str(position), headers=HEADERS, files={ 'file': (file_path.name, blob) })
    if response.status_code == 200:
      return response.json()
    print(f'error in file blob api {response.content.decode("utf-8")} status code: {response.status_code}')
    return None

def finish_file_upload_api(file_name: str, project_name: str):
  response = requests.post(BASE_URL + "/video_ia/file/finish/" + project_name + "/" + file_name, headers=HEADERS)
  if response.status_code == 200:
    return response.json()
  print(f'error in file upload api {response.content.decode("utf-8")} status code: {response.status_code}')
  return None

# funciones de control interno

valid_title_regex = re.compile(r'[^* <>":|\x2F\x5C]+[A-Za-z0-9á-úÁ-Ú ]+')

def clean_title(title:str):
  validations = valid_title_regex.search(title).group()
  # jpin al valid text
  # valid_title = ''
  # for validation in validations:
  #   valid_title = valid_title + ' ' + validation
  return validations

def get_file_prefix_job(job_info: dict, key: str):
  try:
    return job_info['file_prefix'][key]
  except KeyError:
    return ""

def dealocate_threads():
  global threads
  threads_keys = list(threads.keys())
  for key in threads_keys:
    t = threads[key]
    if not t.is_alive():
      # t.join()
      del threads[key]

def can_upload_file_new_file():
  global uploading_files
  with lock:
    return uploading_files < FILES_UPLOAD_MAX_QUANTITY

def can_run_thread():
  global threads_alive
  with lock:
    return threads_alive < MAX_ALIVE_THREADS

def write_file(file_path: Path, content: str):
  with open(file_path, 'w', encoding='utf-8') as file:
    file.write(content)

def upload_file(file_path: Path, project_name: str, size: int):
  file_name = file_path.name
  if size > BLOB_SIZE:
    logging.info(f"file is more than 30MB: {file_name}")
    logging.info(f"initializing file upload: {file_name}")
    result = initialize_file_upload_api(file_name, project_name, size)
    print(result)

    for i in range(0, size, BLOB_SIZE):
      logging.info(f"uploading blob {i}: {file_name}")
      result = upload_file_blob_api(file_path, project_name, i)
      logging.info(f"uploading blob {i} finished: {file_name}")
      print(result)

    result = finish_file_upload_api(file_name, project_name)
    logging.info(f"uploading file ended: {file_name}")
    print(result)
  else:
    result = upload_file_api(file_path, project_name)
    print(result)
  return result

def get_today_date() -> dict:
  date_today = datetime.today()

  return { 
    'day': date_today.day,
    'month': date_today.month,
    'year': date_today.year
  }

def get_output_path(path_base: Path, project: str, year: str, month: str, day: str) -> Path:
  return Path(f"{str(path_base)}/{project}/{year}/{month}/{day}")

def clean_arr(arr: list[str]):
  result_arr = []
  for a in arr:
    if a not in ['/', '\\']:
      result_arr.append(a)
  return result_arr

def process_file(file_path: Path):
  global threads_alive, uploading_files
  jobs_state.init_job_id(file_dir_str)
  with lock:
    threads_alive += 1
  logging.info(f"processing file: {str(file_path)}")
  logging.info(f"initializing processing: {Path(file_path)}")
  can_upload_file = False
  file_name = file_path.name
  file_suffix = file_path.suffix
  file_dir_str = str(file_path)
  path_arr = re.split(r"(\u005C|\u002F)", file_dir_str)
  path_arr.reverse()
  path_arr = clean_arr(path_arr)
  today_date = get_today_date()
  day = today_date['day']
  month = today_date['month']
  year = today_date['year']
  project_name = path_arr[1]
  job_id = None

  destination_path_results = get_output_path(Path(OUTPUT_TEXT_FILES), project_name, year, month, day)
  destination_path_file = Path(f"{OUTPUT_FILES}/{project_name}")

  file_size = file_path.stat().st_size

  while not can_upload_file:
    time.sleep(2)
    can_upload_file = can_upload_file_new_file()
  jobs_state.jobs[file_dir_str].set_job_status('in_progress')
  with lock:
    uploading_files += 1
  jobs_state.jobs[file_dir_str].set_job_status('uploading')
  jobs_state.save_state()
  logging.info(f"uploading file: {str(file_path)}")
  job_info = upload_file(file_path, project_name, file_size)
  logging.info(f"file uploaded: {str(file_path)}")
  with lock:
    uploading_files -= 1

  if job_info == None:
    return
  job_id = job_info['job_id']
  
  jobs_state.jobs[file_dir_str].set_job_id(job_id)
  
  jobs_state.jobs[file_dir_str].set_job_step('processing')
  jobs_state.save_state()
  job_info = get_job_api(job_id)
  job_status = 'initialized'
  print(job_id)
  while job_status != 'finished':
    if job_info == None:
      continue
    job_status = job_info['step']
    jobs_state.jobs[file_dir_str].set_job_step(job_status)
    print(job_status)
    time.sleep(15)
    job_info = get_job_api(job_id)
  
  results_keys = job_info['results']
  
  title = clean_title(get_job_results_api(job_id, 'title'))

  for key in results_keys:
    if key == 'title':
      continue
    if key == 'transcription':
      destination_transcription = destination_path_results / "transcripciones"
      destination_transcription.mkdir(parents=True, exist_ok=True)
      transcription_content = get_job_results_api(job_id, key)
      
      write_file(destination_transcription / f"transcripcion_{title}.md", transcription_content)
      continue
    result_content = get_job_results_api(job_id, key)
    file_prefix = get_file_prefix_job(job_info, key)
    destination_ressult_path = destination_path_results / key
    destination_ressult_path.mkdir(parents=True, exist_ok=True)
    write_file(destination_ressult_path/f"{file_prefix}{title}.md", result_content)

    destination_path_file.mkdir(parents=True, exist_ok=True)
    try:
      # shutil.move(file_path, destination_path_file)
      shutil.copy(file_path, destination_path_file)
      os.remove(file_path)
    except Exception as e:
      pass

    with lock:
      threads_alive -= 1
      if threads_alive < 0:
        threads_alive = 0
    logging.info(f"thread finished: {str(file_path)}")

def get_multimedia_files(path: Path, pattern: str = '*', include_dirs:bool = False ):
  files = path.glob(pattern)
  multimedia_files = []
  for file in files:
    if file.is_dir():
      if include_dirs:
        multimedia_files.append(file)
      sub_files = get_multimedia_files(file, pattern, include_dirs)
      for sub_f in sub_files:
        multimedia_files.append(sub_f)
    else:
      multimedia_files.append(file)
  return multimedia_files

def check_new_file(file_path_str: str):
  file_path = Path(file_path_str)
  if file_path.is_dir():
    return None
  file_suffix = file_path.suffix.lower()
  if file_suffix not in ALLOWED_FILES:
    return None
  return file_path

class ChangeHandler(FileSystemEventHandler):
  def on_modified(self, event):
    print(f"Archivo modificado: {event.src_path}")
    
  def on_created(self, event):
    # logging.info(f"new file: {event.src_path}")
    files_tail.append(event.src_path)
    # logging.info(f"added file to the queue: {files_tail}")
    print(f"Archivo creado: {event.src_path}")
    
  def on_deleted(self, event):
    # logging.info(f"file removed: {event.src_path}")
    print(f"Archivo eliminado: {event.src_path}")

def init_before_observe():
  logging.info(f"initializing...")
  logging.info(f"looking for new files...")
  miltimedia_files = get_multimedia_files(Path(INPUT_FILES))
  logging.info(f"found {len(miltimedia_files)} files")

  for file in miltimedia_files:
    file_path = check_new_file(str(file))
    if file_path == None:
      logging.info(f"file not allowed: {str(file)}")
      continue

    file_path_str = str(file_path)
    print(file_path_str)
    logging.info(f"initializing processing: {file_path_str}")

    while not can_run_thread():
      time.sleep(2)
    
    t = Thread(target=process_file, args=(file_path,))
    logging.info(f"starting thread: {file_path_str}")
    t.start()
    logging.info(f"thread started: {file_path_str}")

    threads[file_path_str] = t


    #if file_path != None:
    #  process_file(file_path)

def main():
  user = get_me_api()
  logging.info(f"script statrted")
  if user == None:
    logging.error(f"error al obtener informacion del usuario")
    logging.error(f"token no valido")
    print("Error al obtener informacion del usuario: token no valido")
    exit(1)

  raw_path = INPUT_FILES
  event_handler = ChangeHandler()
  observer = Observer()
  observer.schedule(event_handler, raw_path, recursive=True)

  init_before_observe()

  observer.start()
  try:
    while True:
      print("waiting for new files...")
      time.sleep(1)
      if len(files_tail) > 0:
        file_path = files_tail.pop(0)
        file_path_str = str(file_path)
        print(file_path_str)
        logging.info(f"initializing processing: {file_path_str}")

        while not can_run_thread():
          time.sleep(2)
        logging.info(f"starting thread: {file_path_str}")
        t = Thread(target=process_file, args=(Path(file_path),))
        t.start()
        logging.info(f"started thread: {file_path_str}")
        threads[file_path_str] = t
  except KeyboardInterrupt:
    observer.stop()
  observer.join()

if __name__ == "__main__":
  main()
