#!/usr/bin/env python3.5

import logging
import inotify.adapters
import os.path
import os
import glob
import shutil
import subprocess
import re
import sys
import psutil
from datetime import datetime, timedelta
from pprint import pprint

BU_SYNC_DIR   = "/data/Backup/Sync"
BU_BACKUP_DIR = "/data/Backup/Backups"
BU_FREQUENCY  = 3550

#_DEFAULT_LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
_DEFAULT_LOG_FORMAT = '%(asctime)s [%(levelname)s] %(message)s'

_LOGGER = logging.getLogger(__name__)

SCHEDULE = [
	{ 'delta': '1h', 'approx': '10m', 'duration': '1D' },
	{ 'delta': '2h', 'approx': '10m', 'duration': '1W' },
	{ 'delta': '1D', 'approx':  '4h', 'duration': '1M' },
	{ 'delta': '1W', 'approx':  '1D', 'duration': '6M' },
	{ 'delta': '1M', 'approx':  '2D', 'duration': '2Y' },
	{ 'delta': '6M', 'approx':  '2W', 'duration': '5Y' },
]

TIMESPECS = {
	's': 1,
	'm': 60,
	'h': 60*60,
	'D': 24*60*60,
	'W': 7*24*60*60,
	'M': 730*60*60,
	'Q': 2190*60*60,
	'Y': 365*24*60*60,
}

class BackupError(Exception):
	def __init__(self,message):
		self.msg = message
	def __str__(self):
		return self.msg

def schedule2time(schedule):
	times = []
	now = datetime.utcnow()
	delta_prev = timedelta(0)
	for s in schedule:
		delta  = parse_period(s['delta'])
		period = parse_period(s['duration'])
		approx = parse_period(s['approx'])

		start = now + delta_prev
		end   = start + period

		times.append([start,end,delta])

		delta_prev = delta





def parse_period(string):
	sl = list(string)
	digits = ''

	# get numerical prefix
	i=0
	while i<len(sl):
		if sl[i].isspace(): 
			next
		if sl[i].isdigit():
			digits.append(sl[i])
		else:
			break
	
	# only digits in the string without sepcifier, assume seconds
	if i==len(sl):
		timespec = 's'
	else:
		timespec = sl[i]
		if not timespec in TIMESPECS.keys():
			raise TypeError('No time sprecifier specified')
	
	sec = int(digits) * TIMESPEC[timespec]
	delta = timedelta(seconds=sec)

	return delta


def get_stamp(stampdir):
	stampfile = os.path.join(stampdir, 'stamp')

	# read first line of stamp file
	with open(stampfile, mode='rt') as f:
		line = f.readline()
	f.close()

	# extract timestamp (first field of line) and compare to others
	stamp = int(line.split().pop(0))
	assert (stamp > 0)

	dt = datetime.utcfromtimestamp(stamp)
	return dt


def find_backups(name):
	bu_basedir = os.path.join(BU_BACKUP_DIR,name)
	bu_dirs    = glob.glob( os.path.join(bu_basedir,'*') )

	_LOGGER.info("Found existing backup dirs: %s", bu_dirs)

	prev_backups = [ ]

	# time of each backup is held in a file called "stamp"
	# its first line has a first field with a unix timestamp
	for bu_dir in bu_dirs:
		try:
			dt = get_stamp(bu_dir)
		except OSError as e:
			_LOGGER.info("Couldn't open stamp in dir `%s': %s", bu_dir, e.strerror)
		except AssertionError:
			_LOGGER.info("Invalid timestamp in dir `%s'", bu_dir)
		else:
			prev_backups.append([ bu_dir, dt ])

	# sort by age
	prev_backups.sort(key=lambda backup: backup[ 1 ])

	return prev_backups


def prune_backups(backups,schedule):
	pprint(backups)
	times = schedule2time(schedule)



def new_backup(path):
	synced_path = os.path.abspath(path)
	assert( os.path.dirname(synced_path) == BU_SYNC_DIR )
	bu_name = os.path.basename(synced_path)

	_LOGGER.info("Found backup for `%s'", bu_name)

	# copy newly synced dir to backups
	dt = get_stamp(synced_path)
	# TODO: iets met timezones.  otherwise, will use utc
	timestr = dt.strftime('%Y-%m-%d_%H:%M')
	new_backup_dir = os.path.join(BU_BACKUP_DIR, bu_name, timestr)
	if os.path.exists(new_backup_dir):
		raise BackupError("Destination path `%s' already exists, refusing to overwrite" % new_backup_dir)

	_LOGGER.info("Copying `%s' to `%s'", synced_path, new_backup_dir)
	# shutil.copytree(synced_path, new_backup, copy_function=os.link)
	subprocess.run([ "/bin/cp", "-al", synced_path, new_backup_dir ])
	_LOGGER.info("Done")

	#backups = find_backups(bu_name)
	#prune_backups(backups,SCHEDULE)


	return


def _configure_logging():
	_LOGGER.setLevel(logging.DEBUG)

	ch = logging.StreamHandler()

	formatter = logging.Formatter(_DEFAULT_LOG_FORMAT)
	ch.setFormatter(formatter)

	_LOGGER.addHandler(ch)


def _main():
	prune_backups("miranda",SCHEDULE)
	sys.exit(0)

	# set nice and ionice for current process
	os.setpriority(os.PRIO_PROCESS, 0, 10)
	p = psutil.Process(os.getpid())
	p.ionice(psutil.IOPRIO_CLASS_IDLE)

	i = inotify.adapters.Inotify()

	d = os.path.join(BU_SYNC_DIR, 'miranda')

	i.add_watch(d.encode('utf-8'))

	try:
		for event in i.event_gen():
			if event is None:
				continue

			(header, type_names, watch_path, filename) = event
			watch_path = watch_path.decode('utf-8')
			filename = filename.decode('utf-8')
			if filename != 'stamp':
				continue

			if header.mask & (inotify.constants.IN_MOVED_TO | inotify.constants.IN_CLOSE_WRITE):
				# only got here if a backup just finished
				_LOGGER.info("WD=(%d) MASK=(%d) COOKIE=(%d) LEN=(%d) MASK->NAMES=%s "
				             "WATCH-PATH=[%s] FILENAME=[%s]",
				             header.wd, header.mask, header.cookie, header.len, type_names,
				             watch_path, filename)
				try:
					new_backup(watch_path)
				except BackupError as e:
					_LOGGER.error(e)

	finally:
		i.remove_watch(d)


if __name__ == '__main__':
	_configure_logging()
	_main()

# vim:ts=4:noexpandtab:sw=4
