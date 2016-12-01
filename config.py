# Replay streaming delay in steps (0.1 seconds)
LIVE_DELAY = 3000

# Database config
DATABASE = dict(
    db="faf_test",
    host="192.168.10.148",
    port=3306,
    user="root",
    password="banana"
)

# Address to bind to
LISTEN_ADDRESS = ''
LISTEN_PORT = 15000

#CONTROL_PORT = 4040
CONTROL_PORT = None

# Root folder to store replays
REPLAY_FOLDER = "vault/replay_vault/"
# Live games streams into files here
STREAMING_FOLDER = REPLAY_FOLDER + "streaming/"
# Contains processed replays which didn't get into database for some reason
PENDING_FOLDER = REPLAY_FOLDER + "pending/"
# Valid replay formats are: legacy, zip, zip_async
REPLAY_FORMAT = 'legacy'
# interval between each disk flush in seconds
FLUSH_INTERVAL = 30
