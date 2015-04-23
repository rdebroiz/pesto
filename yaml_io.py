from concurrent.futures.thread import threading
from log import quit_with_error

try:
    import yaml
except ImportError as err:
    quit_with_error("Pesto requiered PyYaml to be installed, "
                    "checkout requirement.txt.")


class YamlIO():

    _LOCK = threading.Lock()

    @classmethod
    def load_yaml(cls, yaml_filename):
        """
        Load a single yaml document from a file in a thread safe context.
        """
        try:
            with cls._LOCK:
                yaml_doc = yaml.load(open(yaml_filename, 'r'))
        except OSError as oserr:
            quit_with_error("Unable to open file {0}:"
                            "\n{1}".format(yaml_filename, oserr))
        except (yaml.YAMLError, yaml.scanner.ScannerError) as yamlerr:
            quit_with_error("Error while parsing file {0}:"
                            "\n{1}".format(yaml_filename, yamlerr))
        return yaml_doc

    @classmethod
    def load_all_yaml(cls, yaml_filename):
        """
        Load a list of yaml documents from a file in a thread safe context.
        """
        try:
            with cls._LOCK:
                yaml_docs = yaml.load_all(open(yaml_filename, 'r'))
            # stream.close()
        except OSError as oserr:
            quit_with_error("Unable to open file {0}:"
                            "\n{1}".format(yaml_filename, oserr))
        except (yaml.YAMLError, yaml.scanner.ScannerError) as yamlerr:
            quit_with_error("Error while parsing file {0}:"
                            "\n{1}".format(yaml_filename, yamlerr))
        return list(yaml_docs)

    @classmethod
    def dump_yaml(cls, to_dump, yaml_filename):
        """
        Dump a python object inside a yaml document in a thread safe context
        """
        try:
            with cls._LOCK:
                yaml.dump(to_dump, open(yaml_filename, 'w'),
                          default_flow_style=False)
        except OSError as oserr:
            quit_with_error("Unable to open file {0}:"
                            "\n{1}".format(yaml_filename, oserr))
        except (yaml.critical.YAMLError) as yamlerr:
            quit_with_error("Unable to dum {} in {}:\n"
                            "{2}".format(to_dump, yaml_filename, yamlerr))
