class ConfigurationError(Exception):
    def __init__(self, description):
        super().__init__()
        self.msg = description