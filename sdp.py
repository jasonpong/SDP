from impala.dbapi import connect
import yaml
import keyring
import subprocess
import pandas as pd


class SDP:
    def __init__(self, config_path: str, engine_type: str):
        """
        Initialize connection with config from YAML file.

        Args:
            config_path: Path to YAML config file
            engine_type: Either 'impala' or 'hive'
        """
        self.engine_type = engine_type.lower()
        if self.engine_type not in ['impala', 'hive']:
            raise ValueError("Engine type must be either 'impala' or 'hive'")

        # Load config
        with open(config_path) as f:
            all_config = yaml.safe_load(f)
            self.config = all_config.get(engine_type)
            if not self.config:
                raise ValueError(f"No configuration found for engine type: {engine_type}")

        # Validate required config keys
        required_keys = ['host', 'port']
        missing = [k for k in required_keys if k not in self.config]
        if missing:
            raise ValueError(f"Missing required config keys: {', '.join(missing)}")

        self.conn = None
        self.cursor = None

    def set_password(self, password: str):
        """
        Store password securely in system keyring.

        Args:
            password: Password to store
        """
        service_name = f'sdp_{self.engine_type}'
        username = self.config.get('user', 'default')
        keyring.set_password(service_name, username, password)

    def _get_password(self) -> str:
        """Retrieve password from system keyring."""
        service_name = f'sdp_{self.engine_type}'
        username = self.config.get('user', 'default')
        password = keyring.get_password(service_name, username)
        if not password:
            raise ValueError(f"No password found in keyring for {service_name} user {username}")
        return password

    def _kinit(self):
        """Acquire Kerberos ticket from keytab if configured."""
        keytab = self.config.get('keytab_path')
        principal = self.config.get('kerberos_principal')
        if keytab and principal:
            try:
                subprocess.run(
                    ['kinit', '-kt', keytab, principal],
                    check=True,
                    capture_output=True,
                    text=True
                )
            except subprocess.CalledProcessError as e:
                raise RuntimeError(f"kinit failed: {e.stderr.strip()}")
            except FileNotFoundError:
                raise RuntimeError("kinit not found. Is Kerberos client installed?")

    def connect(self):
        """Establish connection using impyla."""
        if not self.conn:
            auth = self.config.get('auth_mechanism', 'LDAP')

            conn_kwargs = dict(
                host=self.config['host'],
                port=self.config['port'],
                auth_mechanism=auth,
                database=self.config.get('database', 'default'),
                use_ssl=self.config.get('use_ssl', True)
            )

            if auth == 'GSSAPI':
                self._kinit()
                conn_kwargs['kerberos_service_name'] = self.config.get(
                    'kerberos_service_name', 'impala'
                )
            else:
                conn_kwargs['user'] = self.config.get('user')
                conn_kwargs['password'] = self._get_password()

            try:
                self.conn = connect(**conn_kwargs)
                self.cursor = self.conn.cursor()
            except Exception as e:
                self.conn = None
                self.cursor = None
                raise ConnectionError(f"Failed to connect to {self.engine_type}: {e}")

    def query(self, sql: str) -> list:
        """Execute query and return results."""
        if not self.cursor:
            self.connect()
        try:
            self.cursor.execute(sql)
        except Exception:
            self.close()
            self.connect()
            self.cursor.execute(sql)
        return self.cursor.fetchall()

    def query_df(self, sql: str) -> pd.DataFrame:
        """Execute query and return results as a DataFrame."""
        if not self.cursor:
            self.connect()
        try:
            self.cursor.execute(sql)
        except Exception:
            self.close()
            self.connect()
            self.cursor.execute(sql)
        columns = [desc[0] for desc in self.cursor.description]
        data = self.cursor.fetchall()
        return pd.DataFrame(data, columns=columns)

    def close(self):
        """Close connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        self.cursor = None
        self.conn = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
