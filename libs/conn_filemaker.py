import os
import jpype
import jaydebeapi
from typing import Optional


class Filemaker:
    def __init__(
        self,
        server: str,
        port: int,
        database: str,
        user: str,
        password: str,
        fmjdbc_jar_path: str = "libs/fmjdbc.jar"
    ):
        self.server = server
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.fmjdbc_jar_path = os.path.abspath(fmjdbc_jar_path)

        self.conn: Optional[jaydebeapi.Connection] = None
        self.cursor: Optional[jaydebeapi.Cursor] = None

        self._start_jvm()

    def _start_jvm(self):
        if not jpype.isJVMStarted():
            jpype.startJVM(classpath=[self.fmjdbc_jar_path], convertStrings=True)

    def __enter__(self):
        self.cursor = self.get_cursor()
        if self.cursor is None:
            raise RuntimeError("Failed to obtain FileMaker cursor — connection or driver failed.")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def get_cursor(self):
        try:
            driver = "com.filemaker.jdbc.Driver"
            url = f"jdbc:filemaker://{self.server}:{self.port}/{self.database}"

            jvm = jpype.getDefaultJVMPath()
            if not jpype.isJVMStarted():
                jpype.startJVM(jvm, f"-Djava.class.path={self.fmjdbc_jar_path}")

            self.conn = jaydebeapi.connect(
                driver,
                url,
                [self.user, self.password],
                self.fmjdbc_jar_path
            )
            self.conn.jconn.setReadOnly(True)
            self.cursor = self.conn.cursor()
        except Exception as e:
            raise Exception(f"FileMaker JDBC connection failed: {e}")
        return self.cursor
