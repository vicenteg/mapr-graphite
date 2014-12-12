import datetime
from datetime import timedelta
import subprocess
import sys
import platform
import json
from string import Template

hostname = platform.node()
end = datetime.datetime.now()
start = end - timedelta(seconds=300)
metric_template = Template('cluster.$node.$grouping.$obj.$metric $value $timestamp')

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

for d in metrics['data']:
	timestamp = int(d['TIMESTAMP']) / 1000
	node = d['NODE']

	if 'CPUS' in d.keys():
		for cpu,metrics in d['CPUS'].items():
			for (name, value) in metrics.items():
				print metric_template.substitute(node=node, grouping='cpu', obj=cpu, metric=name, value=value, timestamp=timestamp)
	
	if 'DISKS' in d.keys():
		for disk,metrics in d['DISKS'].items():
			for (name,value) in metrics.items():
				print metric_template.substitute(node=node, grouping='disk', obj=disk, metric=name, value=value, timestamp=timestamp)

	if 'NETWORK' in d.keys():
		for interface,metrics in d['NETWORK'].items():
			for (name,value) in metrics.items():
				print metric_template.substitute(node=node, grouping='net', obj=interface, metric=name, value=value, timestamp=timestamp)

	if 'MEMORYUSED' in d.keys():
		print metric_template.substitute(node=node, grouping='os', obj='memory', metric='used', value=d['MEMORYUSED'], timestamp=timestamp)
