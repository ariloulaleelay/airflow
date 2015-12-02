from future import standard_library
standard_library.install_aliases()
from builtins import str
from pyhocon import ConfigFactory
from pyhocon.exceptions import ConfigException
import errno
import logging
import os
import sys
import textwrap


try:
    from cryptography.fernet import Fernet
except:
    pass


def generate_fernet_key():
    try:
        FERNET_KEY = Fernet.generate_key().decode()
    except NameError:
        FERNET_KEY = "cryptography_not_found_storing_passwords_in_plain_text"
    return FERNET_KEY


def expand_env_var(env_var):
    """
    Expands (potentially nested) env vars by repeatedly applying
    `expandvars` and `expanduser` until interpolation stops having
    any effect.
    """
    while True:
        interpolated = os.path.expanduser(os.path.expandvars(str(env_var)))
        if interpolated == env_var:
            return interpolated
        else:
            env_var = interpolated


class AirflowConfigException(Exception):
    pass


DEFAULT_CONFIG = """\
airflow {
    core {
        // The home folder for airflow, default is ~/airflow
        airflow_home = ${AIRFLOW_HOME}

        // The folder where your airflow pipelines live, most likely a
        // subfolder in a code repository
        dags_folder = ${airflow.core.airflow_home}"/dags"

        // The folder where airflow should store its log files
        base_log_folder = ${airflow.core.airflow_home}"/logs"

        // The executor class that airflow should use. Choices include
        // SequentialExecutor, LocalExecutor, CeleryExecutor
        executor = SequentialExecutor

        // The SqlAlchemy connection string to the metadata database.
        // SqlAlchemy supports many different database engine, more information
        // their website
        sql_alchemy_conn = "sqlite:///"${airflow.core.airflow_home}"/airflow.db"

        // The amount of parallelism as a setting to the executor. This defines
        // the max number of task instances that should run simultaneously
        // on this airflow installation
        parallelism = 32

        // Whether to load the examples that ship with Airflow. It's good to
        // get started, but you probably want to set this to False in a production
        // environment
        load_examples = True

        // Where your Airflow plugins are stored
        plugins_folder = ${airflow.core.airflow_home}"/plugins"

        // Secret key to save connection passwords in the db
        fernet_key = "" 

        // Whether to disable pickling dags
        donot_pickle = False

        unit_test_mode = False

        security = None

        plugins = []
    }

    pools {
        default {
            sample_pool {
                slots = 1
                description = "Sample pool"
            }
        }
    }

    webserver {
        // The base url of your website as airflow cannot guess what domain or
        // cname you are using. This is use in automated emails that
        // airflow sends to point links to the right web server
        base_url = "http://localhost:8080"

        // The ip specified when starting the web server
        web_server_host = 0.0.0.0

        // The port on which to run the web server
        web_server_port = 8080

        // Secret key used to run your flask app
        secret_key = temporary_key

        // number of threads to run the Gunicorn web server
        threads = 4

        // Expose the configuration file in the web server
        expose_config = true

        // Set to true to turn on authentication : http://pythonhosted.org/airflow/installation.html//web-authentication
        authenticate = False

        // Filter the list of dags by owner name (requires authentication to be enabled)
        filter_by_owner = False

        demo_mode = False

        secret_key = airflowified
    }

    smtp {
        // If you want airflow to send emails on retries, failure, and you want to
        // the airflow.utils.send_email function, you have to configure an smtp
        // server here
        smtp_host = localhost
        smtp_starttls = True
        smtp_user = airflow
        smtp_port = 25
        smtp_password = airflow
        smtp_mail_from = airflow@airflow.com
    }

    celery {
        // This section only applies if you are using the CeleryExecutor in
        // [core] section above

        // The app name that will be used by celery
        celery_app_name = airflow.executors.celery_executor

        // The concurrency that will be used when starting workers with the
        // "airflow worker" command. This defines the number of task instances that
        // a worker will take, so size up your workers based on the resources on
        // your worker box and the nature of your tasks
        celeryd_concurrency = 16

        // When you start an airflow worker, airflow starts a tiny web server
        // subprocess to serve the workers local log files to the airflow main
        // web server, who then builds pages and sends them to users. This defines
        // the port on which the logs are served. It needs to be unused, and open
        // visible from the main web server to connect into the workers.
        worker_log_server_port = 8793

        // The Celery broker URL. Celery supports RabbitMQ, Redis and experimentally
        // a sqlalchemy database. Refer to the Celery documentation for more
        // information.
        broker_url = "sqla+mysql://airflow:airflow@localhost:3306/airflow"

        // Another key Celery setting
        celery_result_backend = "db+mysql://airflow:airflow@localhost:3306/airflow"

        // Celery Flower is a sweet UI for Celery. Airflow has a shortcut to start
        // it `airflow flower`. This defines the port that Celery Flower runs on
        flower_port = 5555

        // Default queue that tasks get assigned to and that worker listen on.
        default_queue = default
    }

    scheduler {

        authenticate = False

        // Task instances listen for external kill signal (when you clear tasks
        // from the CLI or the UI), this defines the frequency at which they should
        // listen (in seconds).
        job_heartbeat_sec = 5

        // The scheduler constantly tries to trigger new tasks (look at the
        // scheduler section in the docs for more information). This defines
        // how often the scheduler should run (in seconds).
        scheduler_heartbeat_sec = 5

        // Statsd (https://github.com/etsy/statsd) integration settings
        statsd_on =  False
        statsd_host =  localhost
        statsd_port =  8125
        statsd_prefix = airflow

    }

    mesos {
        // Mesos master address which MesosExecutor will connect to.
        master = localhost:5050

        // The framework name which Airflow scheduler will register itself as on mesos
        framework_name = Airflow

        // Number of cpu cores required for running one task instance using
        // 'airflow run <dag_id> <task_id> <execution_date> --local -p <pickle_id>'
        // command on a mesos slave
        task_cpu = 1

        // Memory in MB required for running one task instance using
        // 'airflow run <dag_id> <task_id> <execution_date> --local -p <pickle_id>'
        // command on a mesos slave
        task_memory = 256

        // Enable framework checkpointing for mesos
        // See http://mesos.apache.org/documentation/latest/slave-recovery/
        checkpoint = False

        // Enable framework authentication for mesos
        // See http://mesos.apache.org/documentation/latest/configuration/
        authenticate = False

        // Mesos credentials, if authentication is enabled
        // default_principal = admin
        // default_secret = admin
    }

    kerberos {
        ccache = "/tmp/airflow_krb5_ccache"
        principal = airflow
        reinit_frequency = 3600'
        kinit_path = kinit
        keytab = airflow.keytab
    }
}

"""

TEST_CONFIG = """\
airflow {
    core {
        airflow_home = ${AIRFLOW_HOME}
        dags_folder = ${airflow.core.airflow_home}"/dags"
        base_log_folder = ${airflow.core.airflow_home}"/logs"
        executor = SequentialExecutor
        sql_alchemy_conn = "sqlite:///"${airflow.core.airflow_home}"/unittests.db"
        unit_test_mode = True
        load_examples = True
        donot_pickle = False
    }

    webserver {
        base_url = "http://localhost:8080"
        web_server_host = 0.0.0.0
        web_server_port = 8080
    }

    smtp {
        smtp_host = localhost
        smtp_user = airflow
        smtp_port = 25
        smtp_password = airflow
        smtp_mail_from = airflow@airflow.com
    }

    celery {
        celery_app_name = airflow.executors.celery_executor
        celeryd_concurrency = 16
        worker_log_server_port = 8793
        broker_url = "sqla+mysql://airflow:airflow@localhost:3306/airflow"
        celery_result_backend = "db+mysql://airflow:airflow@localhost:3306/airflow"
        flower_port = 5555
        default_queue = default
    }

    scheduler {
        job_heartbeat_sec = 1
        scheduler_heartbeat_sec = 5
        authenticate = true
    }
}
"""


class ConfigEntry(object):

    def __init__(self, config, key):
        self._config = config
        self._key = key

    def __str__(self):
        return self._config.get(self._key)

    def __call__(self, key=None):
        try:
            if key is None:
                return self._config.get(self._key)
            return ConfigEntry(self._config, self._key + '.' + str(key))
        except ConfigException, e:
            raise AirflowConfigException(str(e))

    def __getattr__(self, key):
        try:
            if key == 'as_string':
                return self._config.get_string(self._key)
            elif key == 'as_bool':
                return self._config.get_boolean(self._key)
            elif key == 'as_int':
                return self._config.get_int(self._key)
            elif key == 'as_config':
                return self._config.get_config(self._key)

            return ConfigEntry(self._config, self._key + '.' + key)
        except ConfigException, e:
            raise AirflowConfigException(str(e))


class ConfigParserWithDefaults(object):

    def __init__(self, defaults, config_path):
        file_config = ConfigFactory.parse_file(config_path)
        default_config = ConfigFactory.parse_string(defaults)
        self._config = file_config.with_fallback(default_config)

    def __getattr__(self, key):
        return ConfigEntry(self._config, key)

    def get_config(self, key):
        try:
            return self._config.get_config(key)
        except ConfigException, e:
            raise AirflowConfigException(str(e))

    def get(self, key, additional_key=None):
        if additional_key is not None:
            # NOTE legacy behaviour
            key = str('airflow.' + key + '.' + additional_key).lower()

        try:
            return self._config.get(key)
        except ConfigException, e:
            raise AirflowConfigException(str(e))

    def getboolean(self, key, additional_key=None):
        if additional_key is not None:
            # NOTE legacy behaviour
            key = str('airflow.' + key + '.' + additional_key).lower()
        try:
            return self._config.get_bool(key, False)
        except ConfigException, e:
            raise AirflowConfigException(str(e))

    def getint(self, key, additional_key=None):
        if additional_key is not None:
            # NOTE legacy behaviour
            key = str('airflow.' + key + '.' + additional_key).lower()
        try:
            return self._config.get_int(key)
        except ConfigException, e:
            raise AirflowConfigException(str(e))


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise AirflowConfigException('Had trouble creating a directory')

"""
Setting AIRFLOW_HOME and AIRFLOW_CONFIG from environment variables, using
"~/airflow" and "~/airflow/airflow.conf" respectively as defaults.
"""

if 'AIRFLOW_HOME' not in os.environ:
    os.environ['AIRFLOW_HOME'] = '~/airflow'

AIRFLOW_HOME = expand_env_var(os.environ['AIRFLOW_HOME'])

mkdir_p(AIRFLOW_HOME)

if 'AIRFLOW_CONFIG' not in os.environ:
    if os.path.isfile(expand_env_var('~/airflow.conf')):
        AIRFLOW_CONFIG = expand_env_var('~/airflow.conf')
    else:
        AIRFLOW_CONFIG = AIRFLOW_HOME + '/airflow.conf'
else:
    AIRFLOW_CONFIG = expand_env_var(os.environ['AIRFLOW_CONFIG'])

os.environ['AIRFLOW_HOME'] = AIRFLOW_HOME 
os.environ['AIRFLOW_CONFIG'] = AIRFLOW_CONFIG

if not os.path.isfile(AIRFLOW_CONFIG):
    """
    These configuration options are used to generate a default configuration
    when it is missing. The right way to change your configuration is to alter
    your configuration file, not this code.
    """
    FERNET_KEY = generate_fernet_key()
    logging.info("Creating new config file in: " + AIRFLOW_CONFIG)
    f = open(AIRFLOW_CONFIG, 'w')
    if os.environ.get('AIRFLOW_UNITTESTS', False):
        f.write(TEST_CONFIG)
    else:
        f.write(DEFAULT_CONFIG)
    f.close()

TEST_CONFIG_FILE = AIRFLOW_HOME + '/unittests.conf'
if not os.path.isfile(TEST_CONFIG_FILE):
    logging.info("Creating new config file in: " + TEST_CONFIG_FILE)
    f = open(TEST_CONFIG_FILE, 'w')
    f.write(TEST_CONFIG)
    f.close()

logging.info("Reading the config from " + AIRFLOW_CONFIG)


def test_mode():
    conf = ConfigParserWithDefaults(DEFAULT_CONFIG, TEST_CONFIG_FILE)

conf = ConfigParserWithDefaults(DEFAULT_CONFIG, AIRFLOW_CONFIG)

if 'cryptography' in sys.modules and not conf.has_option('core', 'fernet_key'):
    logging.warning(textwrap.dedent("""

        Your system supports encrypted passwords for Airflow connections but is
        currently storing them in plaintext! To turn on encryption, add a
        "fernet_key" option to the "core" section of your airflow.conf file,
        like this:

            airflow.core.fernet_key = <YOUR FERNET KEY>

        Your airflow.conf file is located at: {cfg}.
        If you need to generate a fernet key, you can run this code:

            from airflow.configuration import generate_fernet_key
            generate_fernet_key()

        """.format(cfg=AIRFLOW_CONFIG)))
