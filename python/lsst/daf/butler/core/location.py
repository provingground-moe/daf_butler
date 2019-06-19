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

__all__ = ("Location", "LocationFactory", "ButlerURI", "S3Location", "S3LocationFactory")

import os
import os.path
import urllib
import posixpath
from pathlib import Path, PurePath, PurePosixPath
import copy

# Determine if the path separator for the OS looks like POSIX
IS_POSIX = os.sep == posixpath.sep

# Root path for this operating system
OS_ROOT_PATH = Path().resolve().root


def os2posix(ospath):
    """Convert a local path description to a POSIX path description.

    Parameters
    ----------
    path : `str`
        Path using the local path separator.

    Returns
    -------
    posix : `str`
        Path using POSIX path separator
    """
    if IS_POSIX:
        return ospath

    posix = PurePath(ospath).as_posix()

    # PurePath strips trailing "/" from paths such that you can no
    # longer tell if a path is meant to be referring to a directory
    # Try to fix this.
    if ospath.endswith(os.sep) and not posix.endswith(posixpath.sep):
        posix += posixpath.sep

    return posix


def posix2os(posix):
    """Convert a POSIX path description to a local path description.

    Parameters
    ----------
    posix : `str`
        Path using the POSIX path separator.

    Returns
    -------
    ospath : `str`
        Path using OS path separator
    """
    if IS_POSIX:
        return posix

    posixPath = PurePosixPath(posix)
    paths = list(posixPath.parts)

    # Have to convert the root directory after splitting
    if paths[0] == posixPath.root:
        paths[0] = OS_ROOT_PATH

    # Trailing "/" is stripped so we need to add back an empty path
    # for consistency
    if posix.endswith(posixpath.sep):
        paths.append("")

    return os.path.join(*paths)


class ButlerURI:
    """Convenience wrapper around URI parsers.

    Provides access to URI components and can convert file
    paths into absolute path URIs. Scheme-less URIs are treated as if
    they are local file system paths and are converted to absolute URIs.

    Parameters
    ----------
    uri : `str` or `urllib.parse.ParseResult`
        URI in string form.  Can be scheme-less if referring to a local
        filesystem path.
    root : `str`, optional
        When fixing up a relative path in a ``file`` scheme or if scheme-less,
        use this as the root. Must be absolute.  If `None` the current
        working directory will be used.
    forceAbsolute : `bool`, optional
        If `True`, scheme-less relative URI will be converted to an absolute
        path using a ``file`` scheme. If `False` scheme-less URI will remain
        scheme-less and will not be updated to ``file`` or absolute path.
    """

    def __init__(self, uri, root=None, forceAbsolute=True):
        if isinstance(uri, str):
            parsed = urllib.parse.urlparse(uri)
        elif isinstance(uri, urllib.parse.ParseResult):
            parsed = copy.copy(uri)
        else:
            raise ValueError("Supplied URI must be either string or ParseResult")

        parsed = self._fixupFileUri(parsed, root=root, forceAbsolute=forceAbsolute)
        self._uri = parsed

    @property
    def scheme(self):
        """The URI scheme (``://`` is not part of the scheme)."""
        return self._uri.scheme

    @property
    def netloc(self):
        """The URI network location."""
        return self._uri.netloc

    @property
    def path(self):
        """The path component of the URI."""
        return self._uri.path

    @property
    def fragment(self):
        """The fragment component of the URI."""
        return self._uri.fragment

    @property
    def params(self):
        """Any parameters included in the URI."""
        return self._uri.params

    @property
    def query(self):
        """Any query strings included in the URI."""
        return self._uri.query

    def geturl(self):
        """Return the URI in string form.

        Returns
        -------
        url : `str`
            String form of URI.
        """
        return self._uri.geturl()

    def replace(self, **kwargs):
        """Replace components in a URI with new values and return a new
        instance.

        Returns
        -------
        new : `ButlerURI`
            New `ButlerURI` object with updated values.
        """
        return self.__class__(self._uri._replace(**kwargs))

    def updateFile(self, newfile):
        """Update in place the final component of the path with the supplied
        file name.

        Parameters
        ----------
        newfile : `str`
            File name with no path component.

        Notes
        -----
        Updates the URI in place.
        """
        if self.scheme:
            # POSIX
            pathclass = posixpath
        else:
            pathclass = os.path

        dir, _ = pathclass.split(self.path)
        newpath = pathclass.join(dir, newfile)

        self._uri = self._uri._replace(path=newpath)

    def __str__(self):
        return self.geturl()

    @staticmethod
    def _fixupFileUri(parsed, root=None, forceAbsolute=False):
        """Fix up relative paths in file URI instances.

        Parameters
        ----------
        parsed : `~urllib.parse.ParseResult`
            The result from parsing a URI using `urllib.parse`.
        root : `str`, optional
            Path to use as root when converting relative to absolute.
            If `None`, it will be the current working directory. This
            is a local file system path, not a URI.
        forceAbsolute : `bool`
            If `True`, scheme-less relative URI will be converted to an
            absolute path using a ``file`` scheme. If `False` scheme-less URI
            will remain scheme-less and will not be updated to ``file`` or
            absolute path. URIs with a defined scheme will not be affected
            by this parameter.

        Returns
        -------
        modified : `~urllib.parse.ParseResult`
            Update result if a file URI is being handled.

        Notes
        -----
        Relative paths are explicitly not supported by RFC8089 but `urllib`
        does accept URIs of the form ``file:relative/path.ext``. They need
        to be turned into absolute paths before they can be used.  This is
        always done regardless of the ``forceAbsolute`` parameter.

        Scheme-less paths are normalized.
        """
        if not parsed.scheme or parsed.scheme == "file":

            # Replacement values for the URI
            replacements = {}

            if root is None:
                root = os.path.abspath(os.path.curdir)

            if not parsed.scheme:
                # if there was no scheme this is a local OS file path
                # which can support tilde expansion.
                expandedPath = os.path.expanduser(parsed.path)

                # Ensure that this is a file URI if it is already absolute
                if os.path.isabs(expandedPath):
                    replacements["scheme"] = "file"
                    replacements["path"] = os2posix(os.path.normpath(expandedPath))
                elif forceAbsolute:
                    # This can stay in OS path form, do not change to file
                    # scheme.
                    replacements["path"] = os.path.normpath(os.path.join(root, expandedPath))
                else:
                    # No change needed for relative local path staying relative
                    # except normalization
                    replacements["path"] = os.path.normpath(expandedPath)

                # normpath strips trailing "/" which makes it hard to keep
                # track of directory vs file when calling replaceFile
                # put it back.
                if "scheme" in replacements:
                    sep = posixpath.sep
                else:
                    sep = os.sep

                if expandedPath.endswith(os.sep) and not replacements["path"].endswith(sep):
                    replacements["path"] += sep

            elif parsed.scheme == "file":
                # file URI implies POSIX path separators so split as posix,
                # then join as os, and convert to abspath. Do not handle
                # home directories since "file" scheme is explicitly documented
                # to not do tilde expansion.
                if posixpath.isabs(parsed.path):
                    # No change needed
                    return copy.copy(parsed)

                replacements["path"] = posixpath.normpath(posixpath.join(os2posix(root), parsed.path))

                # normpath strips trailing "/" so put it back if necessary
                if parsed.path.endswith(posixpath.sep) and not replacements["path"].endswith(posixpath.sep):
                    replacements["path"] += posixpath.sep

            else:
                raise RuntimeError("Unexpectedly got confused by URI scheme")

            # ParseResult is a NamedTuple so _replace is standard API
            parsed = parsed._replace(**replacements)

        return parsed


class Location:
    """Identifies a location within the `Datastore`.

    Parameters
    ----------
    datastoreRootUri : `ButlerURI` or `str`
        Base URI for this datastore, must include an absolute path.
    path : `str`
        Relative path within datastore.  Assumed to be using the local
        path separator if a ``file`` scheme is being used for the URI,
        else a POSIX separator.
    """

    __slots__ = ("_datastoreRootUri", "_path")

    def __init__(self, datastoreRootUri, path):
        if isinstance(datastoreRootUri, str):
            datastoreRootUri = ButlerURI(datastoreRootUri)
        elif not isinstance(datastoreRootUri, ButlerURI):
            raise ValueError("Datastore root must be a ButlerURI instance")

        if not posixpath.isabs(datastoreRootUri.path):
            raise ValueError(f"Supplied URI must be an absolute path (given {datastoreRootUri}).")

        self._datastoreRootUri = datastoreRootUri

        if self._datastoreRootUri.scheme == "file":
            pathModule = os.path
        else:
            pathModule = posixpath

        if pathModule.isabs(path):
            raise ValueError("Path within datastore must be relative not absolute")

        self._path = path

    def __str__(self):
        return self.uri

    @property
    def uri(self):
        """URI string corresponding to fully-specified location in datastore.
        """
        uriPath = os2posix(self.path)
        return self._datastoreRootUri.replace(path=uriPath).geturl()

    @property
    def path(self):
        """Path corresponding to location.

        This path includes the root of the `Datastore`, but does not include
        non-path components of the root URI.  If a file URI scheme is being
        used the path will be returned with the local OS path separator.
        """
        if not self._datastoreRootUri.scheme:
            # Entirely local file system
            return os.path.normpath(os.path.join(self._datastoreRootUri.path, self.pathInStore))
        elif self._datastoreRootUri.scheme == "file":
            return os.path.normpath(os.path.join(posix2os(self._datastoreRootUri.path), self.pathInStore))
        else:
            return posixpath.join(self._datastoreRootUri.path, self.pathInStore)

    @property
    def pathInStore(self):
        """Path corresponding to location relative to `Datastore` root.

        Uses the same path separator as supplied to the object constructor.
        """
        return self._path

    def updateExtension(self, ext):
        """Update the file extension associated with this `Location`.

        Parameters
        ----------
        ext : `str`
            New extension. If an empty string is given any extension will
            be removed. If `None` is given there will be no change.
        """
        if ext is None:
            return

        path, _ = os.path.splitext(self.pathInStore)

        # Ensure that we have a leading "." on file extension (and we do not
        # try to modify the empty string)
        if ext and not ext.startswith("."):
            ext = "." + ext

        self._path = path + ext


class LocationFactory:
    """Factory for `Location` instances.

    The factory is constructed from the root location of the datastore.
    This location can be a path on the file system (absolute or relative)
    or as a URI.

    Parameters
    ----------
    datastoreRoot : `str`
        Root location of the `Datastore` either as a path in the local
        filesystem or as a URI.  File scheme URIs can be used. If a local
        filesystem path is used without URI scheme, it will be converted
        to an absolute path and any home directory indicators expanded.
        If a file scheme is used with a relative path, the path will
        be treated as a posixpath but then converted to an absolute path.
    """

    def __init__(self, datastoreRoot):
        self._datastoreRootUri = ButlerURI(datastoreRoot, forceAbsolute=True)

    def __str__(self):
        return f"{self.__class__.__name__}@{self._datastoreRootUri}"

    def fromPath(self, path):
        """Factory function to create a `Location` from a POSIX path.

        Parameters
        ----------
        path : `str`
            A standard POSIX path, relative to the `Datastore` root.

        Returns
        -------
        location : `Location`
            The equivalent `Location`.
        """
        if os.path.isabs(path):
            raise ValueError("LocationFactory path must be relative to datastore, not absolute.")
        return Location(self._datastoreRootUri, path)


class S3Location:
    """Identifies a location in the `S3Datastore`.
    """

    __slots__ = ("_scheme", "_bucket", "_datastoreRoot", "_relpath")

    def __init__(self, scheme, bucket, datastoreRoot, relpath, **kwargs):
        # no risks, maximal sanitation
        self._scheme = scheme + '://' if scheme[-3:] != '://' else scheme
        self._bucket = bucket.strip('/') + '/'
        self._datastoreRoot = datastoreRoot.strip('/') + '/'
        self._relpath = relpath.lstrip('/')

    def __str__(self):
        return self.uri()

    @property
    def uri(self):
        """URI corresponding to S3Location.
        """
        # uri.geturl will return only s3:/ not s3://
        return self._scheme + os.path.join(self._bucket, self._datastoreRoot, self._relpath)

    @property
    def bucket(self):
        """Return the bucketname of this S3Location.
        """
        # buckets are special because you only want their name, but
        # path.join will not understand their relationship to rootDir
        # without the ending /
        return self._bucket.strip('/')

    @property
    def path(self):
        """Path corresponding to S3Location.

        This path includes the root of the `S3Datastore`.
        """
        return os.path.join(self._datastoreRoot, self._relpath)

    @property
    def pathInStore(self):
        """Path corresponding to S3Location relative to `S3Datastore` root.
        """
        return self._relpath

    def updateExtension(self, ext):
        """Update the file extension associated with this `S3Location`.

        Parameters
        ----------
        ext : `str`
            New extension. If an empty string is given any extension will
            be removed. If `None` is given there will be no change.
        """
        if ext is None:
            return
        path, _ = os.path.splitext(self._relpath)

        # Ensure that we have a leading "." on file extension (and we do not
        # try to modify the empty string)
        if ext and not ext.startswith("."):
            ext = "." + ext

        self._relpath = path + ext


class S3LocationFactory:
    """Factory for `S3Location` instances.
    """

    def __init__(self, bucket, datastoreRoot):
        """Constructor

        Parameters
        ----------
        bucket : `str`
            Name of the Bucket that is used.
        datastoreRoot : `str`
            Root location of the `S3Datastore` in the Bucket.
        """
        # no chances, maximal sanitation
        self._bucket = bucket.strip('/')
        self._datastoreRoot = datastoreRoot.strip('/')

    def fromUri(self, uri):
        """Factory function to create a `S3Location` from a URI.

        Parameters
        ----------
        uri : `str`
            A valid Universal Resource Identifier.

        Returns
        -------
        location : `S3Location`
            The equivalent `S3Location`.
        """
        if uri is None or not isinstance(uri, str):
            raise ValueError("URI must be a string and not {}".format(uri))

        parsed = urllib.parse.urlparse(uri)
        scheme = parsed.scheme
        bucketname = parsed.netloc
        relpath = parsed.path.lstrip('/')
        dirs = relpath.split('/')
        root = dirs[0]
        relpath = os.path.join(*dirs[1:])

        return S3Location(scheme, bucketname, root, relpath)

    def fromPath(self, path):
        """Factory function to create a `S3Location` from a POSIX-like path.

        Parameters
        ----------
        path : `str`
            A POSIX-like path, relative to the `S3Datastore` root.

        Returns
        -------
        location : `S3Location`
            The equivalent `S3Location`.
        """
        if os.path.isabs(path):
            raise ValueError(('A path whose absolute location is in an S3 bucket '
                             'can not have an absolute path: {}').format(path))

        return self.fromUri('s3://' + os.path.join(self._bucket, self._datastoreRoot, path))
