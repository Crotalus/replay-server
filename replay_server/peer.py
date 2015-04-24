
class ReplayPeer:
    def __init__(self, socket):
        self.socket = socket
        self.address = self.socket.getpeername()

        self.sent_step = 0 # Last step sent

    def __str__(self):
        return 'Peer%s' % (self.address,)

    def send(self, step_data):
        self.socket.send(step_data)
        self.sent_step += 1

    def finish(self):
        "Called to say EOS"
        pass

class ReplayFilePeer:
    def __init__(self, file):
        self.file = file

        self.sent_step = 0 # Last step sent

    def __str__(self):
        return 'Peer( %s )' % self.file.name

    def send(self, step_data):
        self.file.write(step_data)
        self.sent_step += 1

    def finish(self):
        # TODO: Notify hook for a finished replay.
        self.file.close()