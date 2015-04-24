
# Replay streaming delay in steps (0.1 seconds)
LIVE_DELAY = 600

# Database config
DATABASE = dict(
    name=    "bob_schema",
    user=    "bob",
    passwd=  "the_dinosaur"
)

# Address to bind to
LISTEN_ADDRESS = ('', 15000)

# Folder to store replays in
REPLAY_FOLDER = 'replays/'