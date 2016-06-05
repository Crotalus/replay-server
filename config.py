
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

# Folder to store replays in
REPLAY_FOLDER = "vault/replay_vault/"
