# metrics2statsd.py

A program to send mapr node metrics to statsd, which in turn sends them to graphite, which allows you to build custom dashboards for your cluster. I use grafana.

# Installing the statsd agent on cluster nodes

You need python and some python modules. First though, grab the script (skip the `env https_proxy=...` stuff if you don't use a proxy).

```
env https_proxy=http://172.16.1.58:3128 git clone https://vicenteg@github.com/vicenteg/mapr-graphite.git
```

Install argparse, requests, and statsd. argparse is not always needed if you have a newer (than 2.6) python. On CentOS 6 with EPEL, you can use yum to install `python-requests` and `python-argparse`. You will still need to use pip to install `statsd`. Again, skip the proxy bit if you don't use one.

```
export https_proxy=http://172.16.1.58:3128
pip install statsd
```

```
yum -y install python-argparse python-requests
```

Or

```
pip install requests
pip install argparse
```

# Run the script

Run in the foreground first to make sure nothing bad happens. Use one of the mapr-webserver nodes as the argument to `--webserver` and point `--statsd` at the node running statsd. The default credentials are `mapr:mapr`. You probably need to change this. If you run statsd with the `console` backend, you can see what statsd is flushing to carbon.

```
sudo -u mapr python metrics2statsd.py --foreground --statsd 172.16.2.117 --webserver 172.16.2.97
```

If you get some data in your graphite instance, you're good to go! Restart the collector as a daemon.


```
sudo -u mapr python metrics2statsd.py --start --statsd 172.16.2.117 --webserver 172.16.2.97
```

Now go forth and create your MapR dashboard.
