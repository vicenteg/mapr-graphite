import socket
import datetime
from datetime import timedelta
import sys
import platform
import sys
import time
from string import Template
import logging
import re

from daemon import Daemon

try:
	import argparse
except ImportError:
	print >>sys.stderr, "Please install the argparse module."
	sys.exit(1)

try:
	import requests
except ImportError:
	print >>sys.stderr, "Please install the requests module."
	sys.exit(1)

logging.basicConfig(
	filename='/opt/mapr/logs/metrics2graphite.log',
	level=logging.DEBUG,
	format='%(asctime)s %(name)s %(levelname)s %(message)s')
logger = logging.getLogger('metrics2graphite')

def get_metrics(webserver_url, username, password, params):
	try:
		logger.debug("getting metrics from '%s' - params = %s" % (webserver_url, params))
		r = requests.get(webserver_url, auth=(username,password), verify=False, params=params)
	except requests.exceptions.ConnectionError as error:
		print >>sys.stderr, "Error connecting: %s" % error
		logger.warn("Connection error: %s" % error)
		raise

	try:
		r.raise_for_status()
	except requests.exceptions.HTTPError as error:
		print >>sys.stderr, "Request was not successful: %s" % error
		logger.error("HTTP error getting metrics from '%s' - %s" % (webserver_url, error))
		sys.exit(1)

	response = r.json()
	logger.debug("Got some JSON, 'data' key has %d objects", len(response['data']))
	data = response['data']
	return data

def main():
	schema = "https"

	parser = argparse.ArgumentParser(
		description='Obtain the ansible inventory from a running MapR cluster.')
	parser.add_argument('--webserver', type=str,
		help='the hostname or IP of a node running mapr-webserver.')
	parser.add_argument('--no-ssl', default=False, action='store_true',
		help='do not use SSL to connect.')
	parser.add_argument('--port', default=8443, type=int,
		help='set the port number to connect to.')
	parser.add_argument('--username', default='mapr', type=str,
		help='MCS username')
	parser.add_argument('--password', default='mapr', type=str,
		help='MCS password')
	group = parser.add_mutually_exclusive_group(required=True)
	group.add_argument('--foreground', action='store_true')
	group.add_argument('--start', action='store_true')
	group.add_argument('--stop', action='store_true')
	group.add_argument('--restart', action='store_true')
	parser.add_argument('--nodes', type=str, default=platform.node(),
		help='Node to get metrics for')
	parser.add_argument('--graphite', type=str,
		help='StatsD Host')

	args = parser.parse_args()
	username = args.username
	password = args.password
	webserver = args.webserver
	port = args.port
	nodes = args.nodes
	graphite_host = args.graphite
	if args.no_ssl:
		schema = "http"
	webserver_url = "%s://%s:%d/rest/node/metrics" % (schema, webserver, port)

	m = Metrics2Statsd(graphite_host, 2003, nodes, webserver_url, username, password)
	if args.start:
		m.start()
	elif args.restart:
		m.restart()
	elif args.stop:
		m.stop()
	elif args.foreground:
		m.run()
	

class Metrics2Statsd(Daemon):
	def __init__(self, graphite_host, graphite_port, nodes, webserver_url, username='mapr', password='mapr'):
		self.metric_template = Template('mapr.$cluster.$node.$grouping.$obj.$metric')
		self.graphite_host = graphite_host
		self.graphite_port = graphite_port
		self.webserver_url = webserver_url
		self.nodes = nodes
		self.username = username
		self.password = password
		self.failed_attempts = 0
		self.last_values = { }

		self.cluster_name = self.get_cluster_name()
		super(Metrics2Statsd, self).__init__('/var/run/metrics2graphite.pid', home_dir='/tmp')

	def get_cluster_name(self):
		cluster_name = None
		with file('/opt/mapr/conf/mapr-clusters.conf') as clusters_conf:
			firstline = clusters_conf.readline()
			cluster_name = re.split('\s+', firstline)[0]
			logger.debug("cluster name is '%s'", cluster_name)
		return re.sub('\.', '_', cluster_name)

	def send_to_carbon_udp(self, message):
		logger.debug(message)
		retval = None

		try:
			self.carbon_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
			retval = self.carbon_socket.sendto("%s" % message, (self.graphite_host, self.graphite_port))
		except Exception as e:
			logger.error('Got a socket error: %s', e)
			raise
		return retval
			

	def run(self):
		seconds_delay = 10	

		while True:
			end = datetime.datetime.now()
			start = end - timedelta(seconds=seconds_delay)
			ms_start = int(start.strftime('%s')) * 1000
			ms_end = int(end.strftime('%s')) * 1000
			params = { 'nodes': self.nodes, 'start': ms_start, 'end': ms_end }

			try:
				all_metrics = get_metrics(self.webserver_url, self.username, self.password, params)
				self.failed_attempts = 0
			except requests.exceptions.ConnectionError as error:
				self.failed_attempts += 1
				logger.warn("Error connecting to %s, have experienced %d errors so far.", self.webserver_url, self.failed_attempts)
				if self.failed_attempts > 5:
					print >>sys.stderr, "Failed 5 times, exiting."
					sys.exit(1)
				continue

			if len(all_metrics) > 0:
				for d in all_metrics[-1:]:
					node = d['NODE']
					timestamp = int(d['TIMESTAMP']) / 1000
					for group in ('DISKS','CPUS','NETWORK'):
						if group in d:
							self.group_metrics(group, self.last_values, d)
					try:
						self.send_gauge(self.metric_template.substitute(cluster=self.cluster_name, node=node, grouping='node', obj='memory', metric='used'), d['MEMORYUSED'], timestamp)
					except KeyError as e:
						logger.warn('%s not in metrics data.', e)

					try:
						self.send_gauge(self.metric_template.substitute(cluster=self.cluster_name, node=node, grouping='node', obj='size', metric='avail'), d['SERVAVAILSIZEMB'], timestamp)
					except KeyError as e:
						logger.warn('%s not in metrics data.', e)

					try:
						self.send_gauge(self.metric_template.substitute(cluster=self.cluster_name, node=node, grouping='node', obj='size', metric='used'), d['SERVUSEDSIZEMB'], timestamp)
					except KeyError as e:
						logger.warn('%s not in metrics data.', e)

					try:
						rpccount_metric = self.metric_template.substitute(cluster=self.cluster_name, node=node, grouping='node', obj='rpc', metric='count')
						if rpccount_metric in self.last_values:
							self.send_counter(rpccount_metric, self.last_values[rpccount_metric], d['RPCCOUNT'], timestamp)
						self.last_values[rpccount_metric] = d['RPCCOUNT']
					except KeyError as e:
						logger.warn('%s is not in metrics data.', e)

					try:
						rpcinbytes_metric = self.metric_template.substitute(cluster=self.cluster_name, node=node, grouping='node', obj='rpc', metric='inbytes')
						if rpcinbytes_metric in self.last_values:
							self.send_counter(rpcinbytes_metric, self.last_values[rpcinbytes_metric], d['RPCINBYTES'], timestamp)
						self.last_values[rpcinbytes_metric] = d['RPCINBYTES']
					except KeyError as e:
						logger.warn('%s is not in metrics data.', e)

					try:
						rpcoutbytes_metric = self.metric_template.substitute(cluster=self.cluster_name, node=node, grouping='node', obj='rpc', metric='outbytes')
						if rpcoutbytes_metric in self.last_values:
							self.send_counter(rpcoutbytes_metric, self.last_values[rpcoutbytes_metric], d['RPCOUTBYTES'], timestamp)
						self.last_values[rpcoutbytes_metric] = d['RPCOUTBYTES']
					except KeyError as e:
						logger.warn('%s is not in metrics data.', e)
			time.sleep(seconds_delay)
		

	def group_metrics(self, group, last_values, all_metrics):
		node = all_metrics['NODE']
		timestamp = int(all_metrics['TIMESTAMP']) / 1000

		for (obj, obj_metrics) in all_metrics[group].items():
			for (metric_name, value) in obj_metrics.items():
				metric = self.metric_template.substitute(cluster=self.cluster_name, node=node, grouping=group.lower(), obj=obj, metric=metric_name)
				if metric in last_values:
					self.send_counter(metric, last_values[metric], value, timestamp)
				last_values[metric] = value

	def send_gauge(self, metric, value, timestamp):
		logger.debug("Sending gauge %s, value '%d' to graphite host '%s:%d'", metric, value, self.graphite_host, self.graphite_port)
		self.send_to_carbon_udp("%s %d %d" % (metric, value, timestamp))


	def send_counter(self, metric, last_value, value, timestamp):
		logger.debug("Sending counter %s, value '%d' to graphite host '%s:%d'", metric, value, self.graphite_host, self.graphite_port)
		delta = value - last_value
		self.send_to_carbon_udp("%s %d %d\n" % (metric, delta, timestamp))


if __name__ == "__main__":
	try:
		main()
	except KeyboardInterrupt as e:
		logger.info('Exiting due to %s', e)
		sys.exit(1)
	except Exception as e:
		logger.error('Caught unknown exception %s: %s', type(e), e)
		sys.exit(1)
