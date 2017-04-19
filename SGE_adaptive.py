from __future__ import print_function, division, absolute_import

import atexit
import json
import logging
import os
import socket
import subprocess
import sys
import paramiko
from time import sleep

import click
import argparse

import distributed
from distributed import Scheduler
from distributed.deploy import Adaptive
from distributed.utils import ignoring, open_port
from distributed.http import HTTPScheduler
from distributed.cli.utils import (check_python_3, install_signal_handlers,
                                   uri_from_host_port)
from tornado.ioloop import IOLoop
from concurrent.futures import ThreadPoolExecutor

from config import cluster_queue_master, cluster_output_dir, py_project_path, django_settings_module, source_dir, dworker

logger = logging.getLogger('distributed.scheduler')

class SGECluster(object):

    def __init__(self, scheduler, cluster_queue_master,
                    cluster_output_dir, py_project_path, django_settings_module,
                    source_dir, dworker,
                    executable='dask-worker',
                    name=None, nthreads=1,
                    **kwargs):
        self.scheduler = scheduler
        self.executor = ThreadPoolExecutor(1)
        self.cluster_queue_master = cluster_queue_master
        self.cluster_output_dir = cluster_output_dir
        self.py_project_path = py_project_path
        self.django_settings_module = django_settings_module
        self.source_dir = source_dir
        self.nthreads = nthreads
        self.dworker = dworker
        # address = self.scheduler.address.split('//')[1]
        # self.ip = address.split(':')[0]
        # self.port = address.split(':')[1]

    def scale_up(self, n):
        """
        Bring the total count of workers up to ``n``

        This function/coroutine should bring the total number of workers up to
        the number ``n``.

        This can be implemented either as a function or as a Tornado coroutine.
        """
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        if n == 1:
            n=len(self.cluster_queue_master)
        address = self.scheduler.address.split('//')[1]
        ip = address.split(':')[0]
        port = address.split(':')[1]
        for queue_master, queue, limit in self.cluster_queue_master:
            ssh.connect(queue_master)
            stdout = int(ssh.exec_command("qstat -g d | wc -l")[1].readlines()[0]) -2
            pending  = int(ssh.exec_command("qstat -g d -s p | wc -l")[1].readlines()[0]) -2
            if pending == 0:
                if limit == 0:
                    stdout = -1
                if stdout < limit:
                    nodes_amount=int(n/len(self.cluster_queue_master))
                    if nodes_amount+stdout > limit:
                        nodes_amount=limit-stdout
                    task="""ssh {queue_master} /home/barakor/scripts_ofir/scripts/adaptive/lift_cluster {source_dir} {cluster_output_dir} {py_project_path} {django_settings_module} {nthreads} {ip} {port} {queue} {project} {dworker} {nodes} \
                    """.format(queue_master=queue_master, \
                            source_dir='--source {}'.format(self.source_dir), \
                            cluster_output_dir='--output {}'.format(self.cluster_output_dir) if self.cluster_output_dir else '' , \
                            py_project_path='--PYTHONPATH {}'.format(self.py_project_path), \
                            django_settings_module='--DJANGO_MODULE {}'.format(self.django_settings_module), \
                            nthreads='--nthreads {}'.format(self.nthreads), ip='--ip {}'.format(ip), port='--port {}'.format(port), \
                            queue="--queue {}".format(queue) if queue else '', dworker='--dworker {}'.format(self.dworker),\
                            nodes='--nodes {}'.format(nodes_amount))
                    subprocess.call(task, shell=True)

    def scale_down(self, workers=[]):
        """
        Remove ``workers`` from the cluster

        Given a list of worker addresses this function should remove those
        workers from the cluster.  This may require tracking which jobs are
        associated to which worker address.

        This can be implemented either as a function or as a Tornado coroutine.
        """
        if workers:
            for w in workers:
                self.executor.submit(self.client.kill_task,
                                     self.app.id,
                                     self.scheduler.worker_info[w]['name'],
                                     scale=True)
        else:
#             for w in self.
            pass


@click.command()
@click.option('--port', type=int, default=None, help="Serving port")
# XXX default port (or URI) values should be centralized somewhere
@click.option('--http-port', type=int, default=9786, help="HTTP port")
@click.option('--bokeh-port', type=int, default=8787, help="Bokeh port")
@click.option('--bokeh-internal-port', type=int, default=8788,
              help="Internal Bokeh port")
@click.option('--bokeh/--no-bokeh', '_bokeh', default=True, show_default=True,
              required=False, help="Launch Bokeh Web UI")
@click.option('--host', type=str, default='',
              help="IP, hostname or URI of this server")
@click.option('--show/--no-show', default=False, help="Show web UI")
@click.option('--bokeh-whitelist', default=None, multiple=True,
              help="IP addresses to whitelist for bokeh.")
@click.option('--prefix', type=str, default=None,
              help="Prefix for the bokeh app")
@click.option('--use-xheaders', type=bool, default=False, show_default=True,
              help="User xheaders in bokeh app for ssl termination in header")
@click.option('--pid-file', type=str, default='',
              help="File to write the process PID")
@click.option('--scheduler-file', type=str, default='',
              help="File to write connection information. "
              "This may be a good way to share connection information if your "
              "cluster is on a shared network file system.")


# @click.option('--cluster_output_dir', type=str, default='$HOME',
#               help="Cluster log output dir")
# @click.option('--py_project_path', type=str, default=None,
#               help="The python project path, if more than one is available, separate it with ':'")
# @click.option('--django_settings_module', type=str, default=None,
#               help="if you are using django, state the settings module here")
# @click.option('--source_dir', type=str, default=None,
#               help="Python enviroment dir, please direct to the folder, and not the activation file itself")
@click.option('--nthreads', type=int, default=1,
              help="How many threads would you want, it's recomended to keep it at 1 if it's a shared cluster")
# @click.option('--project', type=str, default=None,
#               help="SGE project name")
# @click.option('--dworker', type=str, default=None,
#               help="dworker directory")
def main(host, port, http_port, bokeh_port, bokeh_internal_port, show, _bokeh,
         bokeh_whitelist, prefix, use_xheaders, pid_file, scheduler_file,
         nthreads):

    if pid_file:
        with open(pid_file, 'w') as f:
            f.write(str(os.getpid()))

        def del_pid_file():
            if os.path.exists(pid_file):
                os.remove(pid_file)
        atexit.register(del_pid_file)

    if sys.platform.startswith('linux'):
        import resource   # module fails importing on Windows
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        limit = max(soft, hard // 2)
        resource.setrlimit(resource.RLIMIT_NOFILE, (limit, hard))

    addr = uri_from_host_port(host, port, 8786)

    loop = IOLoop.current()
    logger.info('-' * 47)

    services = {('http', http_port): HTTPScheduler}
    if _bokeh:
        with ignoring(ImportError):
            from distributed.bokeh.scheduler import BokehScheduler
            services[('bokeh', bokeh_internal_port)] = BokehScheduler
    scheduler = Scheduler(loop=loop, services=services,
                          scheduler_file=scheduler_file)
    cluster = SGECluster(scheduler=scheduler, cluster_queue_master=cluster_queue_master,
                    cluster_output_dir = cluster_output_dir,
                    py_project_path = py_project_path,
                    django_settings_module = django_settings_module,
                    source_dir = source_dir,
                    dworker = dworker, nthreads=nthreads
                    )
    adapative_cluster = Adaptive(scheduler, cluster)
    scheduler.start(addr)


    bokeh_proc = None
    if _bokeh:
        if bokeh_port == 0:          # This is a hack and not robust
            bokeh_port = open_port() # This port may be taken by the OS
        try:                         # before we successfully pass it to Bokeh
            from distributed.bokeh.application import BokehWebInterface
            bokeh_proc = BokehWebInterface(http_port=http_port,
                    scheduler_address=scheduler.address, bokeh_port=bokeh_port,
                    bokeh_whitelist=bokeh_whitelist, show=show, prefix=prefix,
                    use_xheaders=use_xheaders, quiet=False)
        except ImportError:
            logger.info("Please install Bokeh to get Web UI")
        except Exception as e:
            logger.warn("Could not start Bokeh web UI", exc_info=True)

    logger.info('-' * 47)
    try:
        loop.start()
        loop.close()
    finally:
        scheduler.stop()
        if bokeh_proc:
            bokeh_proc.close()

        logger.info("End scheduler at %r", addr)


def go():
    install_signal_handlers()
    check_python_3()
    main()


if __name__ == '__main__':
    go()
