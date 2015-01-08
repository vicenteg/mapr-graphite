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
	import argparse
except ImportError:
	print >>sys.stderr, "Please install the argparse module."
	sys.exit(1)

try:
	import statsd
except ImportError:
	print >>sys.stderr, "Please install the statsd module."
	sys.exit(1)

try:
	import requests
except ImportError:
	print >>sys.stderr, "Please install the requests module."
	sys.exit(1)

def get_metrics(webserver_url, username, password, params):
	try:
		r = requests.get(webserver_url, auth=(username,password), verify=False, params=params)
	except requests.exceptions.ConnectionError as error:
		print >>sys.stderr, "Error connecting: %s" % error
		sys.exit(1)

	try:
		r.raise_for_status()
	except requests.exceptions.HTTPError as error:
		print >>sys.stderr, "Request was not successful: %s" % error
		sys.exit(1)

	response = r.json()
	data = response['data']
	return data

def main():
	seconds_delay = 10	
	schema = "https"

	parser = argparse.ArgumentParser(
		description='Obtain the ansible inventory from a running MapR cluster.')
	parser.add_argument('webserver', type=str,
		help='the hostname or IP of a node running mapr-webserver.')
	parser.add_argument('--no-ssl', default=False, action='store_true',
		help='do not use SSL to connect.')
	parser.add_argument('--port', default=8443, type=int,
		help='set the port number to connect to.')
	parser.add_argument('--username', default='mapr', type=str,
		help='MCS username')
	parser.add_argument('--password', default='mapr', type=str,
		help='MCS password')
	parser.add_argument('--list', action='store_true')
	parser.add_argument('--nodes', type=str, default=platform.node(),
		help='Node to get metrics for')
	parser.add_argument('--statsd', type=str,
		help='StatsD Host')

	args = parser.parse_args()
	username = args.username
	password = args.password
	webserver = args.webserver
	port = args.port
	nodes = args.nodes
	statsd_host = args.statsd
	if args.no_ssl:
		schema = "http"
	webserver_url = "%s://%s:%d/rest/node/metrics" % (schema, webserver, port)
	metric_template = Template('cluster.$node.$grouping.$obj.$metric')

	last_values = { }
	while True:
		end = datetime.datetime.now()
		start = end - timedelta(seconds=seconds_delay)
		ms_start = int(start.strftime('%s')) * 1000
		ms_end = int(end.strftime('%s')) * 1000
		params = { 'nodes': nodes, 'start': ms_start, 'end': ms_end }

		all_metrics = get_metrics(webserver_url, username, password, params)
		if len(all_metrics) > 0:
			for d in all_metrics[-1:]:
				node = d['NODE']
				for group in ('DISKS','CPUS','NETWORK'):
					group_metrics(statsd_host, group, last_values, d, counter=True)
				send_gauge(statsd_host, metric_template.substitute(node=node, grouping='node', obj='memory', metric='used'), d['MEMORYUSED'])	

				rpccount_metric = metric_template.substitute(node=node, grouping='node', obj='rpc', metric='count')
				if rpccount_metric in last_values:
					send_counter(statsd_host, rpccount_metric, last_values[rpccount_metric], d['RPCCOUNT'])	
				last_values[rpccount_metric] = d['RPCCOUNT']

				rpcinbytes_metric = metric_template.substitute(node=node, grouping='node', obj='rpc', metric='inbytes')
				if rpcinbytes_metric in last_values:
					send_counter(statsd_host, rpcinbytes_metric, last_values[rpcinbytes_metric], d['RPCINBYTES'])	
				last_values[rpcinbytes_metric] = d['RPCINBYTES']

				rpcoutbytes_metric = metric_template.substitute(node=node, grouping='node', obj='rpc', metric='outbytes')
				if rpcoutbytes_metric in last_values:
					send_counter(statsd_host, rpcoutbytes_metric, last_values[rpcoutbytes_metric], d['RPCOUTBYTES'])	
				last_values[rpcoutbytes_metric] = d['RPCOUTBYTES']

		time.sleep(seconds_delay)

def group_metrics(statsd_host, group, last_values, all_metrics, statsd_port=8125, counter=True):
	metric_template = Template('cluster.$node.$grouping.$obj.$metric')
	node = all_metrics['NODE']

	for (obj, obj_metrics) in all_metrics[group].items():
		for (metric_name, value) in obj_metrics.items():
			metric = metric_template.substitute(node=node, grouping=group.lower(), obj=obj, metric=metric_name)
			delta = 0
			if metric in last_values:
				if counter:
					send_counter(statsd_host, metric, last_values[metric], value, statsd_port=statsd_port)
				else:
					send_gauge(statsd_host, metric, value, statsd_port=statsd_port)
			last_values[metric] = value

def send_gauge(statsd_host, metric, value, statsd_port=8125):
	statsd_client = statsd.StatsClient(statsd_host, statsd_port)
	statsd_client.gauge(metric, value)


def send_counter(statsd_host, metric, last_value, value, statsd_port=8125):
	statsd_client = statsd.StatsClient(statsd_host, statsd_port)
	delta = value - last_value
	if delta > 0:
		statsd_client.incr(metric, delta)


if __name__ == "__main__": main()
