# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Configuration control."""

__all__ = ("Config", "ConfigSubset")

import collections
import copy
import logging
import pprint
import os
import yaml
import sys
import io
from yaml.representer import Representer

from .s3utils import parsePathToUriElements

try:
    import boto3
except ImportError:
    boto3 = None

import lsst.utils

from lsst.utils import doImport

yaml.add_representer(collections.defaultdict, Representer.represent_dict)


# Config module logger
log = logging.getLogger(__name__)

# PATH-like environment variable to use for defaults.
CONFIG_PATH = "DAF_BUTLER_CONFIG_PATH"


class Loader(yaml.CSafeLoader):
    """YAML Loader that supports file include directives

    Uses ``!include`` directive in a YAML file to point to another
    YAML file to be included. The path in the include directive is relative
    to the file containing that directive.

        storageClasses: !include storageClasses.yaml

    Examples
    --------
    >>> with open("document.yaml", "r") as f:
           data = yaml.load(f, Loader=Loader)

    Notes
    -----
    See https://davidchall.github.io/yaml-includes.html
    """

    def __init__(self, stream):
        super().__init__(stream)
        self._root = os.path.split(stream.name)[0]
        Loader.add_constructor("!include", Loader.include)

    def include(self, node):
        if isinstance(node, yaml.ScalarNode):
            return self.extractFile(self.construct_scalar(node))

        elif isinstance(node, yaml.SequenceNode):
            result = []
            for filename in self.construct_sequence(node):
                result.append(self.extractFile(filename))
            return result

        elif isinstance(node, yaml.MappingNode):
            result = {}
            for k, v in self.construct_mapping(node).items():
                result[k] = self.extractFile(v)
            return result

        else:
            print("Error:: unrecognised node type in !include statement", file=sys.stderr)
            raise yaml.constructor.ConstructorError

    def extractFile(self, filename):
        filepath = os.path.join(self._root, filename)
        log.debug("Opening YAML file via !include: %s", filepath)
        with open(filepath, "r") as f:
            return yaml.load(f, Loader)


class Config(collections.abc.MutableMapping):
    r"""Implements a datatype that is used by `Butler` for configuration
    parameters.

    It is essentially a `dict` with key/value pairs, including nested dicts
    (as values). In fact, it can be initialized with a `dict`.
    This is explained next:

    Config extends the `dict` api so that hierarchical values may be accessed
    with delimited notation or as a tuple.  If a string is given the delimiter
    is picked up from the first character in that string. For example,
    ``foo.getValue(".a.b.c")``, ``foo["a"]["b"]["c"]``, ``foo["a", "b", "c"]``,
    ``foo[".a.b.c"]``, and ``foo["/a/b/c"]`` all achieve the same outcome.
    If the first character is alphanumeric, no delimiter will be used.
    ``foo["a.b.c"]`` will be a single key ``a.b.c`` as will ``foo[":a.b.c"]``.
    Unicode characters can be used as the delimiter for distinctiveness if
    required.

    If a key in the hierarchy starts with a non-alphanumeric character care
    should be used to ensure that either the tuple interface is used or
    a distinct delimiter is always given in string form.

    Finally, the delimiter can be escaped if it is part of a key and also
    has to be used as a delimiter. For example, ``foo[r".a.b\.c"]`` results in
    a two element hierarchy of ``a`` and ``b.c``.  For hard-coded strings it is
    always better to use a different delimiter in these cases.

    Note that adding a multi-level key implicitly creates any nesting levels
    that do not exist, but removing multi-level keys does not automatically
    remove empty nesting levels.  As a result:

        >>> c = Config()
        >>> c[".a.b"] = 1
        >>> del c[".a.b"]
        >>> c["a"]
        Config({'a': {}})

    Storage formats supported:

    - yaml: read and write is supported.


    Parameters
    ----------
    other : `str` or `Config` or `dict`
        Other source of configuration, can be:

        - (`str`) Treated as a path to a config file on disk. Must end with
          ".yaml".
        - (`Config`) Copies the other Config's values into this one.
        - (`dict`) Copies the values from the dict into this Config.

        If `None` is provided an empty `Config` will be created.
    """

    _D = "→"
    """Default internal delimiter to use for components in the hierarchy when
    constructing keys for external use (see `Config.names()`)."""

    includeKey = "includeConfigs"
    """Key used to indicate that another config should be included at this
    part of the hierarchy."""

    def __init__(self, other=None):
        self._data = {}
        self.configFile = None

        if other is None:
            return

        if isinstance(other, Config):
            self._data = copy.deepcopy(other._data)
        elif isinstance(other, collections.abc.Mapping):
            self.update(other)
        elif isinstance(other, str):
            # if other is a string, assume it is a file path.
            self.__initFromFile(other)
            self._processExplicitIncludes()
        else:
            # if the config specified by other could not be recognized raise
            # a runtime error.
            raise RuntimeError("A Config could not be loaded from other:%s" % other)

    def ppprint(self):
        """helper function for debugging, prints a config out in a readable
        way in the debugger.

        use: pdb> print(myConfigObject.ppprint())

        Returns
        -------
        s : `str`
            A prettyprint formatted string representing the config
        """
        return pprint.pformat(self._data, indent=2, width=1)

    def __repr__(self):
        return f"{type(self).__name__}({self._data!r})"

    def __str__(self):
        return self.ppprint()

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        return iter(self._data)

    def copy(self):
        return type(self)(self)

    def __initFromFile(self, path):
        """Load a file from path.

        Parameters
        ----------
        path : `str`
            To a persisted config file.
        """
        if path.endswith("yaml"):
            if path.startswith('s3://'):
                self.__initFromS3File(path)
            else:
                self.__initFromYamlFile(path)
        else:
            raise RuntimeError("Unhandled config file type:%s" % path)

    def __initFromS3File(self, path):
        """Load a file from path pointing to an S3 Bucket.

        Parameters
        ----------
        path : `str`
            To a persisted config file.
        """
        if boto3 is None:
            raise ModuleNotFoundError(('boto3 not found.'
                                       'Are you sure it is installed?'))

        scheme, bucket, key = parsePathToUriElements(path)
        s3 = boto3.client('s3')
        try:
            response = s3.get_object(Bucket=bucket, Key=key)
        except (s3.exceptions.NoSuchKey, s3.exceptions.NoSuchBucket) as err:
            raise FileNotFoundError(f'No such file or directory: {path}') from err
        byteStr = response['Body'].read()
        confDict = yaml.safe_load(byteStr)
        self.update(confDict)

    def __initFromYamlFile(self, path):
        """Opens a file at a given path and attempts to load it in from yaml.

        Parameters
        ----------
        path : `str`
            To a persisted config file in YAML format.
        """
        log.debug("Opening YAML config file: %s", path)
        with open(path, "r") as f:
            self.__initFromYaml(f)
        self.configFile = path

    def __initFromYaml(self, stream):
        """Loads a YAML config from any readable stream that contains one.

        Parameters
        ----------
        stream
            To a persisted config file in YAML format.

        Raises
        ------
        yaml.YAMLError
            If there is an error loading the file.
        """
        content = yaml.load(stream, Loader=Loader)
        if content is None:
            content = {}
        self._data = content
        return self

    def _processExplicitIncludes(self):
        """Scan through the configuration searching for the special
        includeConfigs directive and process the includes."""

        # Search paths for config files
        searchPaths = [os.path.curdir]
        if self.configFile is not None:
            searchPaths.append(os.path.abspath(os.path.dirname(self.configFile)))

        # Ensure we know what delimiter to use
        names = self.nameTuples()
        for path in names:
            if path[-1] == self.includeKey:

                log.debug("Processing file include directive at %s", self._D + self._D.join(path))
                basePath = path[:-1]

                # Extract the includes and then delete them from the config
                includes = self[path]
                del self[path]

                # Be consistent and convert to a list
                if not isinstance(includes, list):
                    includes = [includes]

                # Read each file assuming it is a reference to a file
                # The file can be relative to config file or cwd
                # ConfigSubset search paths are not used
                # At some point these might be URIs which we will have to
                # assume resolve explicitly
                subConfigs = []
                for fileName in includes:
                    found = None
                    if os.path.isabs(fileName):
                        found = fileName
                    else:
                        for dir in searchPaths:
                            filePath = os.path.join(dir, fileName)
                            if os.path.exists(filePath):
                                found = os.path.normpath(os.path.abspath(filePath))
                                break
                    if not found:
                        raise RuntimeError(f"Unable to find referenced include file: {fileName}")

                    # Read the referenced Config as a Config
                    subConfigs.append(type(self)(found))

                # Now we need to merge these sub configs with the current
                # information that was present in this node in the config
                # tree with precedence given to the explicit values
                newConfig = subConfigs.pop(0)
                for sc in subConfigs:
                    newConfig.update(sc)

                # Explicit values take precedence
                if not basePath:
                    # This is an include at the root config
                    newConfig.update(self)
                    # Replace the current config
                    self._data = newConfig._data
                else:
                    newConfig.update(self[basePath])
                    # And reattach to the base config
                    self[basePath] = newConfig

    @staticmethod
    def _splitIntoKeys(key):
        r"""Split the argument for get/set/in into a hierarchical list.

        Parameters
        ----------
        key : `str` or iterable
            Argument given to get/set/in. If an iterable is provided it will
            be converted to a list.  If the first character of the string
            is not an alphanumeric character then it will be used as the
            delimiter for the purposes of splitting the remainder of the
            string. If the delimiter is also in one of the keys then it
            can be escaped using ``\``. There is no default delimiter.

        Returns
        -------
        keys : `list`
            Hierarchical keys as a `list`.
        """
        if isinstance(key, str):
            if not key[0].isalnum():
                d = key[0]
                key = key[1:]
            else:
                return [key, ]
            escaped = f"\\{d}"
            temp = None
            if escaped in key:
                # Complain at the attempt to escape the escape
                doubled = fr"\{escaped}"
                if doubled in key:
                    raise ValueError(f"Escaping an escaped delimiter ({doubled} in {key})"
                                     " is not yet supported.")
                # Replace with a character that won't be in the string
                temp = "\r"
                if temp in key or d == temp:
                    raise ValueError(f"Can not use character {temp!r} in hierarchical key or as"
                                     " delimiter if escaping the delimiter")
                key = key.replace(escaped, temp)
            hierarchy = key.split(d)
            if temp:
                hierarchy = [h.replace(temp, d) for h in hierarchy]
            return hierarchy
        elif isinstance(key, collections.abc.Iterable):
            return list(key)
        else:
            # Not sure what this is so try it anyway
            return [key, ]

    def _getKeyHierarchy(self, name):
        """Retrieve the key hierarchy for accessing the Config

        Parameters
        ----------
        name : `str` or `tuple`
            Delimited string or `tuple` of hierarchical keys.

        Returns
        -------
        hierarchy : `list` of `str`
            Hierarchy to use as a `list`.  If the name is available directly
            as a key in the Config it will be used regardless of the presence
            of any nominal delimiter.
        """
        if name in self._data:
            keys = [name, ]
        else:
            keys = self._splitIntoKeys(name)
        return keys

    def _findInHierarchy(self, keys, create=False):
        """Look for hierarchy of keys in Config

        Parameters
        ----------
        keys : `list` or `tuple`
            Keys to search in hierarchy.
        create : `bool`, optional
            If `True`, if a part of the hierarchy does not exist, insert an
            empty `dict` into the hierarchy.

        Returns
        -------
        hierarchy : `list`
            List of the value corresponding to each key in the supplied
            hierarchy.  Only keys that exist in the hierarchy will have
            a value.
        complete : `bool`
            `True` if the full hierarchy exists and the final element
            in ``hierarchy`` is the value of relevant value.
        """
        d = self._data

        def checkNextItem(k, d, create):
            """See if k is in d and if it is return the new child"""
            nextVal = None
            isThere = False
            if d is None:
                # We have gone past the end of the hierarchy
                pass
            elif isinstance(d, collections.abc.Sequence):
                # Check sequence first because for lists
                # __contains__ checks whether value is found in list
                # not whether the index exists in list. When we traverse
                # the hierarchy we are interested in the index.
                try:
                    nextVal = d[int(k)]
                    isThere = True
                except IndexError:
                    pass
                except ValueError:
                    isThere = k in d
            elif k in d:
                nextVal = d[k]
                isThere = True
            elif create:
                d[k] = {}
                nextVal = d[k]
                isThere = True
            return nextVal, isThere

        hierarchy = []
        complete = True
        for k in keys:
            d, isThere = checkNextItem(k, d, create)
            if isThere:
                hierarchy.append(d)
            else:
                complete = False
                break

        return hierarchy, complete

    def __getitem__(self, name):
        # Override the split for the simple case where there is an exact
        # match.  This allows `Config.items()` to work via a simple
        # __iter__ implementation that returns top level keys of
        # self._data.
        keys = self._getKeyHierarchy(name)

        hierarchy, complete = self._findInHierarchy(keys)
        if not complete:
            raise KeyError(f"{name} not found")
        data = hierarchy[-1]

        if isinstance(data, collections.abc.Mapping):
            data = Config(data)
            # Ensure that child configs inherit the parent internal delimiter
            if self._D != Config._D:
                data._D = self._D
        return data

    def __setitem__(self, name, value):
        keys = self._getKeyHierarchy(name)
        last = keys.pop()
        if isinstance(value, Config):
            value = copy.deepcopy(value._data)

        hierarchy, complete = self._findInHierarchy(keys, create=True)
        if hierarchy:
            data = hierarchy[-1]
        else:
            data = self._data

        try:
            data[last] = value
        except TypeError:
            data[int(last)] = value

    def __contains__(self, key):
        keys = self._getKeyHierarchy(key)
        hierarchy, complete = self._findInHierarchy(keys)
        return complete

    def __delitem__(self, key):
        keys = self._getKeyHierarchy(key)
        last = keys.pop()
        hierarchy, complete = self._findInHierarchy(keys)
        if complete:
            if hierarchy:
                data = hierarchy[-1]
            else:
                data = self._data
            del data[last]
        else:
            raise KeyError(f"{key} not found in Config")

    def update(self, other):
        """Like dict.update, but will add or modify keys in nested dicts,
        instead of overwriting the nested dict entirely.

        For example, for the given code:
        foo = {"a": {"b": 1}}
        foo.update({"a": {"c": 2}})

        Parameters
        ----------
        other : `dict` or `Config`
            Source of configuration:

            - If foo is a dict, then after the update foo == {"a": {"c": 2}}
            - But if foo is a Config, then after the update
              foo == {"a": {"b": 1, "c": 2}}
        """
        def doUpdate(d, u):
            if not isinstance(u, collections.abc.Mapping) or \
                    not isinstance(d, collections.abc.Mapping):
                raise RuntimeError("Only call update with Mapping, not {}".format(type(d)))
            for k, v in u.items():
                if isinstance(v, collections.abc.Mapping):
                    d[k] = doUpdate(d.get(k, {}), v)
                else:
                    d[k] = v
            return d
        doUpdate(self._data, other)

    def merge(self, other):
        """Like Config.update, but will add keys & values from other that
        DO NOT EXIST in self.

        Keys and values that already exist in self will NOT be overwritten.

        Parameters
        ----------
        other : `dict` or `Config`
            Source of configuration:
        """
        otherCopy = copy.deepcopy(other)
        otherCopy.update(self)
        self._data = otherCopy._data

    def nameTuples(self, topLevelOnly=False):
        """Get tuples representing the name hierarchies of all keys.

        The tuples returned from this method are guaranteed to be usable
        to access items in the configuration object.

        Parameters
        ----------
        topLevelOnly : `bool`, optional
            If False, the default, a full hierarchy of names is returned.
            If True, only the top level are returned.

        Returns
        -------
        names : `list` of `tuple` of `str`
            List of all names present in the `Config` where each element
            in the list is a `tuple` of strings representing the hierarchy.
        """
        if topLevelOnly:
            return list((k,) for k in self)

        def getKeysAsTuples(d, keys, base):
            if isinstance(d, collections.abc.Sequence):
                theseKeys = range(len(d))
            else:
                theseKeys = d.keys()
            for key in theseKeys:
                val = d[key]
                levelKey = base + (key,) if base is not None else (key,)
                keys.append(levelKey)
                if isinstance(val, (collections.abc.Mapping, collections.abc.Sequence)) \
                        and not isinstance(val, str):
                    getKeysAsTuples(val, keys, levelKey)
        keys = []
        getKeysAsTuples(self._data, keys, None)
        return keys

    def names(self, topLevelOnly=False, delimiter=None):
        """Get a delimited name of all the keys in the hierarchy.

        The values returned from this method are guaranteed to be usable
        to access items in the configuration object.

        Parameters
        ----------
        topLevelOnly : `bool`, optional
            If False, the default, a full hierarchy of names is returned.
            If True, only the top level are returned.
        delimiter : `str`, optional
            Delimiter to use when forming the keys.  If the delimiter is
            present in any of the keys, it will be escaped in the returned
            names.  If `None` given a delimiter will be automatically provided.
            The delimiter can not be alphanumeric.

        Returns
        -------
        names : `list` of `str`
            List of all names present in the `Config`.

        Notes
        -----
        This is different than the built-in method `dict.keys`, which will
        return only the first level keys.

        Raises
        ------
        ValueError:
            The supplied delimiter is alphanumeric.
        """
        if topLevelOnly:
            return list(self.keys())

        # Get all the tuples of hierarchical keys
        nameTuples = self.nameTuples()

        if delimiter is not None and delimiter.isalnum():
            raise ValueError(f"Supplied delimiter ({delimiter!r}) must not be alphanumeric.")

        if delimiter is None:
            # Start with something, and ensure it does not need to be
            # escaped (it is much easier to understand if not escaped)
            delimiter = self._D

            # Form big string for easy check of delimiter clash
            combined = "".join("".join(str(s) for s in k) for k in nameTuples)

            # Try a delimiter and keep trying until we get something that
            # works.
            ntries = 0
            while delimiter in combined:
                log.debug(f"Delimiter '{delimiter}' could not be used. Trying another.")
                ntries += 1

                if ntries > 100:
                    raise ValueError(f"Unable to determine a delimiter for Config {self}")

                # try another one
                while True:
                    delimiter = chr(ord(delimiter)+1)
                    if not delimiter.isalnum():
                        break

        log.debug(f"Using delimiter {delimiter!r}")

        # Form the keys, escaping the delimiter if necessary
        strings = [delimiter + delimiter.join(str(s).replace(delimiter, f"\\{delimiter}") for s in k)
                   for k in nameTuples]
        return strings

    def asArray(self, name):
        """Get a value as an array.

        May contain one or more elements.

        Parameters
        ----------
        name : `str`
            Key to use to retrieve value.

        Returns
        -------
        array : `collections.abc.Sequence`
            The value corresponding to name, but guaranteed to be returned
            as a list with at least one element. If the value is a
            `~collections.abc.Sequence` (and not a `str`) the value itself
            will be returned, else the value will be the first element.
        """
        val = self.get(name)
        if isinstance(val, str):
            val = [val]
        elif not isinstance(val, collections.abc.Sequence):
            val = [val]
        return val

    def __eq__(self, other):
        if isinstance(other, Config):
            other = other._data
        return self._data == other

    def __ne__(self, other):
        if isinstance(other, Config):
            other = other._data
        return self._data != other

    #######
    # i/o #

    def dump(self, output):
        """Writes the config to a yaml stream.

        Parameters
        ----------
        output
            The YAML stream to use for output.
        """
        # First a set of known keys is handled and written to the stream in a
        # specific order for readability.
        # After the expected/ordered keys are weritten to the stream the
        # remainder of the keys are written to the stream.
        data = copy.copy(self._data)
        keys = []
        for key in keys:
            try:
                yaml.safe_dump({key: data.pop(key)}, output, default_flow_style=False)
                output.write("\n")
            except KeyError:
                pass
        if data:
            yaml.safe_dump(data, output, default_flow_style=False)

    def dumpToFile(self, path):
        """Writes the config to a file.

        Parameters
        ----------
        path : `str`
            Path to the file to use for output.
        """
        with open(path, "w") as f:
            self.dump(f)

    def dumpToS3(self, bucket, key):
        """Writes the config to a file in S3 Bucket.

        Parameters
        ----------
        bucketname: `str`
            Name of the Bucket into which config will be written.
        key : `str`
            Path to the file to use for output, relative to the bucket.
        """
        if boto3 is None:
            raise ModuleNotFoundError(("Could not find boto3. "
                                       "Are you sure it is installed?"))
        stream = io.StringIO()
        self.dump(stream)
        stream.seek(0)

        s3 = boto3.client('s3')
        s3.put_object(Bucket=bucket, Key=key, Body=stream.read())

    @staticmethod
    def updateParameters(configType, config, full, toUpdate=None, toCopy=None, overwrite=True):
        """Generic helper function for updating specific config parameters.

        Allows for named parameters to be set to new values in bulk, and
        for other values to be set by copying from a reference config.

        Assumes that the supplied config is compatible with ``configType``
        and will attach the updated values to the supplied config by
        looking for the related component key.  It is assumed that
        ``config`` and ``full`` are from the same part of the
        configuration hierarchy.

        Parameters
        ----------
        configType : `ConfigSubset`
            Config type to use to extract relevant items from ``config``.
        config : `Config`
            A `Config` to update. Only the subset understood by
            the supplied `ConfigSubset` will be modified. Default values
            will not be inserted and the content will not be validated
            since mandatory keys are allowed to be missing until
            populated later by merging.
        full : `Config`
            A complete config with all defaults expanded that can be
            converted to a ``configType``. Read-only and will not be
            modified by this method. Values are read from here if
            ``toCopy`` is defined.

            Repository-specific options that should not be obtained
            from defaults when Butler instances are constructed
            should be copied from ``full`` to ``config``.
        toUpdate : `dict`, optional
            A `dict` defining the keys to update and the new value to use.
            The keys and values can be any supported by `Config`
            assignment.
        toCopy : `tuple`, optional
            `tuple` of keys whose values should be copied from ``full``
            into ``config``.
        overwrite : `bool`, optional
            If `False`, do not modify a value in ``config`` if the key
            already exists.  Default is always to overwrite.

        Raises
        ------
        ValueError
            Neither ``toUpdate`` not ``toCopy`` were defined.
        """
        if toUpdate is None and toCopy is None:
            raise ValueError("One of toUpdate or toCopy parameters must be set.")

        # If this is a parent configuration then we need to ensure that
        # the supplied config has the relevant component key in it.
        # If this is a parent configuration we add in the stub entry
        # so that the ConfigSubset constructor will do the right thing.
        # We check full for this since that is guaranteed to be complete.
        if configType.component in full and configType.component not in config:
            config[configType.component] = {}

        # Extract the part of the config we wish to update
        localConfig = configType(config, mergeDefaults=False, validate=False)

        if toUpdate:
            for key, value in toUpdate.items():
                if key in localConfig and not overwrite:
                    log.debug("Not overriding key '%s' with value '%s' in config %s",
                              key, value, localConfig.__class__.__name__)
                else:
                    localConfig[key] = value

        if toCopy:
            localFullConfig = configType(full, mergeDefaults=False)
            for key in toCopy:
                if key in localConfig and not overwrite:
                    log.debug("Not overriding key '%s' from defaults in config %s",
                              key, value, localConfig.__class__.__name__)
                else:
                    localConfig[key] = localFullConfig[key]

        # Reattach to parent if this is a child config
        if configType.component in config:
            config[configType.component] = localConfig
        else:
            config.update(localConfig)


class ConfigSubset(Config):
    """Config representing a subset of a more general configuration.

    Subclasses define their own component and when given a configuration
    that includes that component, the resulting configuration only includes
    the subset.  For example, your config might contain ``schema`` if it's
    part of a global config and that subset will be stored. If ``schema``
    can not be found it is assumed that the entire contents of the
    configuration should be used.

    Default values are read from the environment or supplied search paths
    using the default configuration file name specified in the subclass.
    This allows a configuration class to be instantiated without any
    additional arguments.

    Additional validation can be specified to check for keys that are mandatory
    in the configuration.

    Parameters
    ----------
    other : `Config` or `str` or `dict`
        Argument specifying the configuration information as understood
        by `Config`
    validate : `bool`, optional
        If `True` required keys will be checked to ensure configuration
        consistency.
    mergeDefaults : `bool`, optional
        If `True` defaults will be read and the supplied config will
        be combined with the defaults, with the supplied valiues taking
        precedence.
    searchPaths : `list` or `tuple`, optional
        Explicit additional paths to search for defaults. They should
        be supplied in priority order. These paths have higher priority
        than those read from the environment in
        `ConfigSubset.defaultSearchPaths()`.
    """

    component = None
    """Component to use from supplied config. Can be None. If specified the
    key is not required. Can be a full dot-separated path to a component.
    """

    requiredKeys = ()
    """Keys that are required to be specified in the configuration.
    """

    defaultConfigFile = None
    """Name of the file containing defaults for this config class.
    """

    def __init__(self, other=None, validate=True, mergeDefaults=True, searchPaths=None):

        # Create a blank object to receive the defaults
        # Once we have the defaults we then update with the external values
        super().__init__()

        # Create a standard Config rather than subset
        externalConfig = Config(other)

        # Select the part we need from it
        # To simplify the use of !include we also check for the existence of
        # component.component (since the included files can themselves
        # include the component name)
        if self.component is not None:
            doubled = (self.component, self.component)
            # Must check for double depth first
            if doubled in externalConfig:
                externalConfig = externalConfig[doubled]
            elif self.component in externalConfig:
                externalConfig._data = externalConfig._data[self.component]

        # Default files read to create this configuration
        self.filesRead = []

        # Assume we are not looking up child configurations
        containerKey = None

        # Sometimes we do not want to merge with defaults.
        if mergeDefaults:

            # Supplied search paths have highest priority
            fullSearchPath = []
            if searchPaths:
                fullSearchPath.extend(searchPaths)

            # Read default paths from enviroment
            fullSearchPath.extend(self.defaultSearchPaths())

            # There are two places to find defaults for this particular config
            # - The "defaultConfigFile" defined in the subclass
            # - The class specified in the "cls" element in the config.
            #   Read cls after merging in case it changes.
            if self.defaultConfigFile is not None:
                self._updateWithConfigsFromPath(fullSearchPath, self.defaultConfigFile)

            # Can have a class specification in the external config (priority)
            # or from the defaults.
            pytype = None
            if "cls" in externalConfig:
                pytype = externalConfig["cls"]
            elif "cls" in self:
                pytype = self["cls"]

            if pytype is not None:
                try:
                    cls = doImport(pytype)
                except ImportError:
                    raise RuntimeError("Failed to import cls '{}' for config {}".format(pytype,
                                                                                        type(self)))
                defaultsFile = cls.defaultConfigFile
                if defaultsFile is not None:
                    self._updateWithConfigsFromPath(fullSearchPath, defaultsFile)

                # Get the container key in case we need it
                try:
                    containerKey = cls.containerKey
                except AttributeError:
                    pass

        # Now update this object with the external values so that the external
        # values always override the defaults
        self.update(externalConfig)

        # If this configuration has child configurations of the same
        # config class, we need to expand those defaults as well.

        if mergeDefaults and containerKey is not None and containerKey in self:
            for idx, subConfig in enumerate(self[containerKey]):
                self[containerKey, idx] = type(self)(other=subConfig, validate=validate,
                                                     mergeDefaults=mergeDefaults,
                                                     searchPaths=searchPaths)

        if validate:
            self.validate()

    @classmethod
    def defaultSearchPaths(cls):
        """Read the environment to determine search paths to use for global
        defaults.

        Global defaults, at lowest priority, are found in the ``config``
        directory of the butler source tree. Additional defaults can be
        defined using the environment variable ``$DAF_BUTLER_CONFIG_PATHS``
        which is a PATH-like variable where paths at the front of the list
        have priority over those later.

        Returns
        -------
        paths : `list`
            Returns a list of paths to search. The returned order is in
            priority with the highest priority paths first. The butler config
            directory will always be at the end of the list.
        """
        # We can pick up defaults from multiple search paths
        # We fill defaults by using the butler config path and then
        # the config path environment variable in reverse order.
        defaultsPaths = []

        if CONFIG_PATH in os.environ:
            externalPaths = os.environ[CONFIG_PATH].split(os.pathsep)
            defaultsPaths.extend(externalPaths)

        # Find the butler configs
        defaultsPaths.append(os.path.join(lsst.utils.getPackageDir("daf_butler"), "config"))

        return defaultsPaths

    def _updateWithConfigsFromPath(self, searchPaths, configFile):
        """Search the supplied paths, merging the configuration values

        The values read will override values currently stored in the object.
        Every file found in the path will be read, such that the earlier
        path entries have higher priority.

        Parameters
        ----------
        searchPaths : `list`
            Paths to search for the supplied configFile. This path
            is the priority order, such that files read from the
            first path entry will be selected over those read from
            a later path.
        configFile : `str`
            File to locate in path. If absolute path it will be read
            directly and the search path will not be used.
        """
        if os.path.isabs(configFile):
            if os.path.exists(configFile):
                self.filesRead.append(configFile)
                self._updateWithOtherConfigFile(configFile)
        else:
            # Reverse order so that high priority entries
            # update the object last.
            for pathDir in reversed(searchPaths):
                file = os.path.join(pathDir, configFile)
                if os.path.exists(file):
                    self.filesRead.append(file)
                    self._updateWithOtherConfigFile(file)

    def _updateWithOtherConfigFile(self, file):
        """Read in some defaults and update.

        Update the configuration by reading the supplied file as a config
        of this class, and merging such that these values override the
        current values. Contents of the external config are not validated.

        Parameters
        ----------
        file : `Config`, `str`, or `dict`
            Entity that can be converted to a `ConfigSubset`.
        """
        # Use this class to read the defaults so that subsetting can happen
        # correctly.
        externalConfig = type(self)(file, validate=False, mergeDefaults=False)
        self.update(externalConfig)

    def validate(self):
        """Check that mandatory keys are present in this configuration.

        Ignored if ``requiredKeys`` is empty."""
        # Validation
        missing = [k for k in self.requiredKeys if k not in self._data]
        if missing:
            raise KeyError(f"Mandatory keys ({missing}) missing from supplied configuration for {type(self)}")
