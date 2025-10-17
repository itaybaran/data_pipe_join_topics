import yaml, sys, logging


class Config:
    def __init__(self, config_file_path):
        self.file_path = config_file_path
        self.config = {}
        self.load_config()

    def load_config(self):
        try:
            with open(self.file_path, encoding='utf-8') as yaml_file:
                self.config = yaml.full_load(yaml_file)

        except IOError as e:
            print("IO error:", sys.exc_info()[0])
        except:
            print("Unexpected error:", sys.exc_info()[0])



    def get_log_dir_path(self):
        return self.config["logger"]["log_dir_path"]

    def get_log_name(self):
        return self.config["logger"]["log_name"]

    def get_log_level(self):
        level = logging.DEBUG
        if self.config["logger"]["log_level"] == "ERROR":
            level = logging.ERROR
        else:
            if self.config["logger"]["log_level"] == "INFO":
                level = logging.INFO
            else:
                if self.config["logger"]["log_level"] == "WARNING":
                    level = logging.WARNING
        return level

    def get_config(self):
        return self.config