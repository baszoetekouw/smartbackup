#!/usr/bin/env python3.5
# vim:ts=4:noexpandtab:sw=4:tw=120

import sys
if sys.version_info[0] != 3 or sys.version_info[1] < 5:
	print("This script requires Python version 3.5")
	sys.exit(1)

import logging
import inotify.adapters
import os.path
import os
import glob
import shutil
import subprocess
import re
import psutil
import operator
import argparse
from copy import copy, deepcopy
from datetime import datetime, timedelta
from pprint import pprint


####################################################
## configuration variables
####################################################

BU_SYNC_DIR   = "/data/Backup/Sync"
BU_BACKUP_DIR = "/data/Backup/Backups"
BU_NAMES      = ('regan','miranda')

SCHEDULE = [
	{ 'delta': '1h', 'period':  '2D' },
	{ 'delta': '2h', 'period':  '1W' },
	{ 'delta': '1D', 'period':  '1M' },
	{ 'delta': '1W', 'period':  '6M' },
	{ 'delta': '1M', 'period':  '2Y' },
	{ 'delta': '3M', 'period':  '5Y' },
	{ 'delta': '6M', 'period': '50Y' },
]

####################################################
## end of configuration
####################################################

_DEFAULT_LOG_FORMAT = '%(asctime)s [%(levelname)s] %(message)s'
#_DEFAULT_LOG_FORMAT = '%(message)s'
_LOGGER = None

TIMESPEC = {
	's':             1,
	'm':            60,
	'h':         60*60,
	'D':      24*60*60,
	'W':    7*24*60*60,
	'M':   30*24*60*60,
	'Q': 13*7*24*60*60,
	'Y':  365*24*60*60,
}

# generic exception
class BackupError(Exception):
	def __init__(self,message):
		self.msg = message
	def __str__(self):
		return self.msg

# print a nice timedelta:
def timedelta_format(td):
	seconds = int(td.total_seconds())
	periods = [
		('year',   60*60*24*365),
		('month'  ,60*60*24*30),
		('week',   60*60*24*7),
		('day',    60*60*24),
		('hour',   60*60),
		('minute', 60),
		('second', 1)
	]

	strings=[]
	for period_name, period_seconds in periods:
		if seconds >= period_seconds:
			period_value, seconds = divmod(seconds,period_seconds)
			if period_value == 1:
				strings.append("%s %s" % (period_value, period_name))
			else:
				strings.append("%s %ss" % (period_value, period_name))

	return ", ".join(strings)

# parse a timespan string (e.g, "1d", "12s", "3Y") to a datetime.timedelta
def parse_period(string):
	# split string
	sl = list(string)
	digits = ''

	# get numerical prefix
	i=0
	while i<len(sl):
		if sl[i].isspace():
			next
		if sl[i].isdigit():
			digits=digits+sl[i]
		else:
			break
		i=i+1

	# only digits in the string without specifier, assume seconds
	if i==len(sl):
		timespec = 's'
	else:
		timespec = sl[i]
		if not timespec in TIMESPEC.keys():
			raise TypeError('No time sprecifier specified')

	sec = int(digits) * TIMESPEC[timespec]
	delta = timedelta(seconds=sec)

	return delta

# parse a backup schedule
def schedule2time(schedule):
	times = []
	start = now = datetime.utcnow()
	delta_prev = timedelta(0)
	for s in schedule:
		name   = '{delta}/{period}'.format(**s)
		delta  = parse_period(s['delta'])
		period = parse_period(s['period'])

		# note2: we are counting backward
		end   = start
		start = now - period
		times.append({"name": name, "start": start, "end": end, "period": period, "delta": delta})

		delta_prev = delta

	return times


# find the names of existing machines
def get_machines(the_dir):
	machines = [d for d in os.listdir(the_dir) if d[0]!='_' and os.path.isdir(os.path.join(the_dir,d))]
	return machines


# find and read the stamp file in a backup dir
def get_stamp(stampdir):
	stampfile = os.path.join(stampdir,'stamp')

	# read first line of stamp file
	f = open(stampfile,mode='rt')
	line = f.readline()
	f.close()

	# extract timestamp (first field of line) and compare to others
	stamp = int( line.split().pop(0) )
	if not stamp>0:
		raise BackupError("Can't read `{}'".format(stampfile))

	dt = datetime.utcfromtimestamp(stamp)
	return dt

# find a list of backups for a particular machine
def find_backups(name):
	bu_basedir = os.path.join(BU_BACKUP_DIR,name)
	bu_dirs    = glob.glob( os.path.join(bu_basedir,'*') )

	#_LOGGER.info("Found existing backup dirs: %s", bu_dirs)

	prev_backups = []

	# time of each backup is held in a file called "stamp"
	# its first line has a first field with a unix timestamp
	for bu_dir in bu_dirs:
		try:
			dt = get_stamp(bu_dir)
		except:
			_LOGGER.info("Couldn't open stamp in dir `%s'", bu_dir)
		else:
			prev_backups.append({"backup": bu_dir, "timestamp": dt})

	# sort by age
	prev_backups.sort(key=lambda backup: backup["timestamp"])

	return prev_backups


# given a list of backups with timeindices, and a list of time intervals,
# return a list of lists of backups which fall in each intervals
def distribute_backups(times,backups):
	buckets = sorted(deepcopy(times),key=lambda b: b['delta'])

	# todo: detect backups that remain unsorted (not in any bucket)
	for bucket in buckets:
		bucket['backups'] = []

	for backup in backups:
		for bucket in buckets:
			if backup['timestamp']>=bucket['start'] and backup['timestamp']<=bucket['end']:
				bucket['backups'].append(backup)

	return buckets


# find which backups can be pruned
def backups_find_prune(times,backups):
	# sort backups by most recent first
	backups   = sorted(deepcopy(backups),key=lambda b: b['timestamp'], reverse=True)
	# sort intervals by most recent first
	intervals = sorted(deepcopy(times),key=lambda i: i['start'], reverse=True)

	now = datetime.utcnow()
	for b in backups:
		if b['timestamp']>now:
			print("Warning; backup `{backup}' in the future, ignoring!".format(**b))
			backups.remove(b)
		else:
			# backups is sorted by timestamp
			break


	# initialize first backup by hand
	backup = backups[0]
	backup['status'] = 'Keep'
	backup['age'] = timedelta(0)
	backup['gap'] = timedelta(0)
	print("  backup from {timestamp} (age=  0.00, gap={gap!s:>18}): {status} ".format(**backup) )

	# initialize counts etc
	intervals_iterator = iter(intervals)
	interval = None  # current interval description
	b = 1            # counter for backups[]
	b_kept = backup  # most recent backup that's Kept

	try:
		while b<len(backups):
			# for convenience
			backup = backups[b]
			b_prev = backups[b-1]

			# check if we need to switch the next backup interval
			while not interval or backup['timestamp']<interval['start']:
				# note: this will raise an exception if we're out of intervals
				interval = next(intervals_iterator)
				#print("starting new interval {name} from {start} to {end}".format(**interval))

			backup["gap"] = b_prev['timestamp']-backup['timestamp']
			backup["age"] = b_kept['timestamp']-backup['timestamp']
			backup["relage"] = backup['age']/interval['delta']

			# in the ideal case, we would like to keep one backup for each interval['delta']
			# in realy, of couse, the backups aren't evenly spaces in time, we need to be a little bit smarter
			# keep the backup if its age is close (<10% of the interval) to the interval delta
			if backup["relage"]>0.65:
				# but first check if the next backup is closer to the optimal point
				# this mainly occurs for long delta's, as in that case, many backups ight exist within
				# 10% of the delta
				if b<len(backups)-1:
					next_age = b_kept['timestamp'] - backups[b+1]["timestamp"]
					if abs(next_age-interval['delta'])<abs(backup["age"]-interval["delta"]):
						# next backup is closer to ideal time
						backup['status']='KeepNext'
					else:
						# this backup is closest to the ideal time
						backup['status'] = 'Keep'
						b_kept = backup
				else:
					# this is the last backup, keep it
					backup['status'] = 'Keep'
					b_kept = backup
			else:
				# backup is too far away from ideal point
				backup['status'] = 'Prune'

			#print("  backup from {timestamp} (age={relage:6.2f}, gap={gap!s:>18s}): {status} ".format(**backup) )
			print("  backup from {timestamp} (age={relage:6.2f}, gap={gap}): {status} ".format(**backup) )
			b=b+1
	except StopIteration:
		# only reached if we're out of intervals
		# all remaining backups are too old to keep
		for backup in backups[b:]:
			backup['status']='Old'
			print("  backup from {timestamp}: {status} ".format(**backup) )

	return backups


# given a backup schedule and a machine name, run a pruning process
def run_prune(schedule,machine):
	_LOGGER.info("Pruning backups for '{}'".format(machine))

	times = schedule2time(schedule)
	backups = find_backups(machine)

	if not backups:
		print("Warning: no backups found for '{}'".format(machine))
		return

	buckets = distribute_backups(times,backups)
	print("total backups: %u" % len(backups))
	for b in buckets:
		print("%6s  %u" % (b['name'],len(b['backups'])))

	print()
	backups = backups_find_prune(times,backups)

	# now do the actual purging
	try:
		olddir = os.path.join(BU_BACKUP_DIR,'_old',machine)
		os.mkdir(olddir)
	except FileExistsError:
		pass

	for b in backups:
		if not b['status']=='Keep':
			print("Moving backup `{backup}' to old".format(**b))
			shutil.move(b['backup'], olddir)

	for b in backups:
		if not b['status']=='Keep':
			oldname = os.path.join(olddir,os.path.basename(b['backup']))
			oldname = os.path.realpath(oldname)
			# supersafe extra check
			if os.path.commonprefix([oldname,"/data/Backup/Backups/_old"])!="/data/Backup/Backups/_old":
				raise Exception("ERRORTJE!")
			print("Removing backup `{}' from old".format(oldname))
			shutil.rmtree(oldname)

	print()

	return


# handle a new, incoming backup
def new_backup(path):
	synced_path = os.path.abspath(path)
	assert( os.path.dirname(synced_path) == BU_SYNC_DIR )
	bu_name = os.path.basename(synced_path)

	_LOGGER.info("Found backup for `%s'", bu_name)

	# copy newly synced dir to backups
	dt = get_stamp(synced_path)
	timestr = dt.strftime('%Y-%m-%d_%H:%M')
	new_backup = os.path.join( BU_BACKUP_DIR, bu_name, timestr )
	if os.path.exists(new_backup):
		raise BackupError("Destination path `%s' already exists, refusing to overwrite" % new_backup)

	_LOGGER.info("Copying `%s' to `%s'", synced_path, new_backup)
	# TODO: this is waaaaaay to slow, for some reason.  find out why
	#shutil.copytree(synced_path, new_backup, copy_function=os.link)
	subprocess.run(["/bin/cp","-al", synced_path, new_backup])
	_LOGGER.info("Done")

	return

def watch_backups(machines):
	# set nice and ionice for current process
	os.setpriority(os.PRIO_PROCESS, 0, 10)
	p = psutil.Process(os.getpid())
	p.ionice(psutil.IOPRIO_CLASS_IDLE)

	i = inotify.adapters.Inotify()

	# TODO watch all paths
	for name in machines:
		d = os.path.join(BU_SYNC_DIR,name)
		i.add_watch( d.encode('utf-8') )
		print( "Start watching {}".format(d) )

	# wait for an inotify event
	for event in i.event_gen():
		if event is None: 
			continue

		# decode event info
		(header, type_names, watch_path, filename) = event
		watch_path = watch_path.decode('utf-8')
		filename   = filename.decode('utf-8')

		# only take action of the stamp file was updated
		if filename != 'stamp': 
			continue

		if header.mask & (inotify.constants.IN_MOVED_TO|inotify.constants.IN_CLOSE_WRITE):
			# only got here if a backup just finished
			_LOGGER.info("WD=(%d) MASK=(%d) COOKIE=(%d) LEN=(%d) MASK->NAMES=%s "
				"WATCH-PATH=[%s] FILENAME=[%s]",
				header.wd, header.mask, header.cookie, header.len, type_names,
				watch_path, filename)
			try:
				# handle the new backup
				new_backup(watch_path)
			except BackupError as e:
				_LOGGER.error(e)

def show_schedule(schedule):
	times = schedule2time(schedule)
	for rule in times:
		period = timedelta_format(rule['period'])
		delta  = timedelta_format(rule['delta' ])
		print("For {}, keep a backup every {}".format(period,delta))


def _configure_logging():
	global _LOGGER
	if not _LOGGER:
		_LOGGER = logging.getLogger(__name__)
		formatter = logging.Formatter(_DEFAULT_LOG_FORMAT)
		ch = logging.StreamHandler()
		ch.setFormatter(formatter)
		_LOGGER.addHandler(ch)
		_LOGGER.setLevel(logging.DEBUG)

def _parse_args():
	parser = argparse.ArgumentParser(description='Simple Backup Script')
	group = parser.add_mutually_exclusive_group(required=True)
	group.add_argument('-m','--monitor', action='store_true',
	                   help='Monitor the sync dir and update backups when a new sync is finished')
	group.add_argument('-p','--prune', action='store_true',
	                   help='Prune old backups')
	group.add_argument('-l','--list', action='store_true',
	                   help='List all machine and the dat of the last backup')
	group.add_argument('-s','--schedule', action='store_true',
	                   help='Show the schedule of which backups to keep')

	args = parser.parse_args()
	return args

def _main():
	_configure_logging()
	args = _parse_args();

	if args.prune or args.monitor:
		if os.geteuid() != 0:
			print("Please run this as root")
			exit(1)

	if args.list:
		sync_machines = get_machines(BU_SYNC_DIR)
		pprint(sync_machines)
	elif args.schedule:
		show_schedule(SCHEDULE)
	elif args.monitor:
		sync_machines = get_machines(BU_SYNC_DIR)
		watch_backups(sync_machines)
	elif args.prune:
		machines = get_machines(BU_BACKUP_DIR)
		for machine in machines:
			run_prune(SCHEDULE,machine)

	exit(0)

if __name__ == '__main__':
	_main()

