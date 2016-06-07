import aiomysql
import logging

log = logging.getLogger(__name__)

class LoggingCursor(aiomysql.Cursor):
    """
    Allows use of cursors using the ``with'' context manager statement.
    """
    def __init__(self, connection, echo=False):
        super().__init__(connection, echo)

    async def execute(self, query, args=None):
        log.debug("Executing query: %s with args: %s", query, args)
        return await super().execute(query, args)

    @property
    def size(self):
        return self.rowcount
