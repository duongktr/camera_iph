class GenericBackend(object):
    def __init__(self, uri=None):
        if uri is not None:
            self.connect(uri=uri)

    def connect(self, config):
        pass

    def get(self, table, _id):
        pass

    def create(self, table, data):
        pass

    def update(self, table, _id, data):
        pass

    def delete(table, _id):
        pass
