import datetime
from datetime import timedelta
import subprocess
import sys
import platform
import sys
import time
import json
from string import Template

try:
	import statsd
except ImportError:
	print >>sys.stderr, "Please install the statsd module."
	sys.exit(1)

def send_metrics_to_statsd(metrics, statsd_host='localhost', statsd_port=8125):
	statsd_client = statsd.StatsClient(statsd_host, statsd_port)
	metric_template = Template('cluster.$node.$grouping.$obj.$metric')

	for d in metrics['data']:
		timestamp = int(d['TIMESTAMP']) / 1000
		node = d['NODE']

		if 'CPUS' in d.keys():
			for cpu,metrics in d['CPUS'].items():
				for (name, value) in metrics.items():
					statsd_client.gauge(metric_template.substitute(node=node, grouping='cpu', obj=cpu, metric=name), value)
		
		if 'DISKS' in d.keys():
			for disk,metrics in d['DISKS'].items():
				for (name,value) in metrics.items():
					statsd_client.gauge(metric_template.substitute(node=node, grouping='disk', obj=disk, metric=name), value)

		if 'NETWORK' in d.keys():
			for interface,metrics in d['NETWORK'].items():
				for (name,value) in metrics.items():
					statsd_client.gauge(metric_template.substitute(node=node, grouping='net', obj=interface, metric=name), value)

		if 'MEMORYUSED' in d.keys():
			statsd_client.gauge(metric_template.substitute(node=node, grouping='os', obj='memory', metric='used'), d['MEMORYUSED'])

def get_metrics(hostname=None, start=None, end=None):
	if hostname == None:
		hostname = platform.node()
	if end == None:
		end = datetime.datetime.now()
	if start == None:
		start = end - timedelta(seconds=300)

	try:
		ms_start = int(start.strftime('%s')) * 1000
		ms_end = int(end.strftime('%s')) * 1000
		command = 'maprcli node metrics -json -nodes %s -start %s -end %s' % (hostname, ms_start, ms_end)
		print >>sys.stderr, command
		p = subprocess.Popen(command.split(' '), stdout=subprocess.PIPE)
		json_metrics = p.communicate()[0]
	except:
		raise

	metrics = json.loads(json_metrics)
	return metrics

def main():
	seconds_delay = 10	

	while True:
		metrics = get_metrics()
		send_metrics_to_statsd(metrics, statsd_host='172.16.2.117', statsd_port=8125)
		time.sleep(seconds_delay)

if __name__ == "__main__": main()
