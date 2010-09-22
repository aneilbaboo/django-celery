"""																																									
																																									   
 Start the celery daemon from the Django management command.																											
																																									   
"""
from django.core.management.base import BaseCommand,make_option
from django.utils.daemonize import become_daemon
from celery.bin.celeryd import run_worker, OPTION_LIST
from os import path,getpid
from string import atoi
from django.conf import settings
	
class Command(BaseCommand):
	"""Run the celery daemon."""
	# Note: the filter of --version avoids a name conflict with																										
	# manage.py's --version option.  Still feels a little yuck.																										
	option_list = BaseCommand.option_list + \
				   (make_option('--daemonize', action='store_true', dest='daemonize',
								help='Runs celeryd as a daemon'),
					make_option('--pidfile', help="Location of the file containing the process id"),
					make_option('--stop', action='store_true', dest='stop',
								help='Stop the currently running celeryd process')) +\
				  filter(lambda opt: '--version' not in opt._long_opts, \
						 OPTION_LIST)
	help = 'Run celeryd as a daemon using django settings'

	def handle(self, *args, **options):
		"""Handle the management command."""
		daemonize = options.get('daemonize')
		pidfile = options.get('pidfile')
		stop = options.get('stop')
		
		# stop existing celeryd
		stopped = stop_celery_process(pidfile)
		if stop:
			if not stopped:
				print "No existing celeryd process"
			quit()
		
		print "Starting celeryd (%s)" % getpid()
		
		# safely turn this process into a daemon
		if daemonize:
			become_daemon()
		
		# write the pidfile
		if pidfile:
			with open(pidfile,'w+') as f:
				print "writing pidfile:",pidfile
				f.write(str(getpid()))
			
		run_worker(**options)

from os import kill,remove
def stop_celery_process(pidfile):
	if pidfile and path.exists(pidfile):
		with open(pidfile,'r') as f:
			pid = f.read()
			try:
				kill(atoi(pid),0)
				print "Stopped celeryd process (%s)" % pid
			except:
				return False
			remove(pidfile)
		return True
	
