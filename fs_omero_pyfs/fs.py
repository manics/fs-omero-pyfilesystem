# https://docs.pyfilesystem.org/en/latest/implementers.html
from fs.base import FS
from fs.enums import ResourceType
from fs.errors import (
    DirectoryExists,
    DirectoryExpected,
    DirectoryNotEmpty,
    FileExpected,
    RemoteConnectionError,
    ResourceError,
    ResourceNotFound,
    Unsupported,
)
from fs.info import Info
from fs.subfs import SubFS

from io import IOBase
import os
import omero.clients
from omero.gateway import BlitzGateway
from omero.rtypes import unwrap

DEFAULT_NS = 'github.com/manics/fs-omero-pyfilesystem'


class OriginalFileObj(omero.gateway._OriginalFileAsFileObj, IOBase):
    # https://docs.python.org/3.6/library/io.html#io.IOBase

    def __init__(self, *args, **kwargs):
        self._readable = kwargs.pop('readable', True)
        self._writable = kwargs.pop('writable', True)
        super().__init__(*args, **kwargs)

    def close(self):
        super().close()
        super(IOBase, self).close()

    def fileno(self):
        raise OSError('fileno not supported')

    def flush(self):
        pass

    def isatty(self):
        return False

    def readable(self):
        return self._readable

    # io.IOBase methods
    # TODO: Make more efficient?

    def readline(self, size=-1):
        line = b''
        while self.pos < self.rfs.size() and (size < 0 or len(line) < size):
            buf = self.read(self.bufsize)
            eol = buf.find(b'\n')
            if eol < 0:
                line += buf
            else:
                line += buf[:eol + 1]
                self.pos -= (len(buf) - eol - 1)
                break
        if size > -1:
            backtrack = len(line) - size
            line = line[:-backtrack]
            self.pos -= backtrack
        return line

    def readlines(self, hint=-1):
        lines = []
        c = 0
        while self.pos < self.rfs.size() and (hint < 0 or c < hint):
            line = self.readline(hint)
            lines.append(line)
            c += len(line)
        return lines

    def seekable(self):
        return True

    def truncate(self, size=0):
        if not self._writable:
            raise PermissionError('File opened read-only')
        self.rfs.truncate(size)

    def write(self, buf):
        if not self._writable:
            raise PermissionError('File opened read-only')
        n = len(buf)
        self.rfs.write(buf, self.pos, n)
        self.pos += n
        return len(buf)

    def writable(self):
        return self._writable

    def writelines(self, lines):
        for line in lines:
            self.write(line)

    def __iter__(self):
        while self.pos < self.rfs.size():
            yield self.readline()

    def __next__(self):
        if self.pos < self.rfs.size():
            return self.readline()
        raise StopIteration


class OmeroFS(FS):

    def __init__(self, *, host, user, passwd, root='/', create=True,
                 ns=DEFAULT_NS, group=None):
        super().__init__()
        self.ns = ns
        # Use omero.client to get a better error message if connect fails
        if group:
            raise RemoteConnectionError(msg='OMERO group not supported')

        try:
            client = omero.client(host)
            client.setAgent('fs-omero-pyfs')
            session = client.createSession(user, passwd)
            assert session
            self.conn = BlitzGateway(client_obj=client, group=group)
            # client.enableKeepAlive(60)
            # group=?
        except Exception as e:
            raise RemoteConnectionError(
                exc=e, msg='Failed to connect to OMERO')
        self.root = root
        if not self._get_dir(root, throw=(not create)):
            self._create_tag(root)

    def _normalise_path(self, path):
        path = '/' + path.strip('/')
        return path

    def _split_basename(self, path):
        path = self._normalise_path(path)
        dirname, basename = path.rsplit('/', 1)
        return self._normalise_path(dirname), basename

    def _get_file(self, path, throw=True):
        dirname, basename = self._split_basename(path)
        if not basename:
            if throw:
                raise FileExpected(path)
        params = omero.sys.ParametersI()
        params.addString('dirname', dirname)
        params.addString('ns', self.ns)
        params.addString('filename', basename)
        files = unwrap(self.conn.getQueryService().projection(
            'SELECT parent.id FROM OriginalFileAnnotationLink '
            'WHERE parent.name=:filename '
            'AND child.textValue=:dirname '
            'AND child.ns=:ns '
            'AND child.class=TagAnnotation',
            params))
        if not files:
            if throw:
                raise ResourceNotFound(path)
            return None
        if len(files) > 1:
            raise ResourceError(
                path, msg='Multiple files [{}] found with same path'.format(
                    len(files)))
        return self.conn.getObject('OriginalFile', files[0][0])

    def _get_dir(self, path, throw=True):
        dirs = list(self.conn.getObjects('TagAnnotation',
                    attributes={'ns': self.ns, 'textValue': path}))
        if not dirs:
            if throw:
                if self._get_file(dir, throw=False):
                    raise DirectoryExpected(path)
                raise ResourceNotFound(path)
            return None
        if len(dirs) > 1:
            raise ResourceError(
                path, msg='Multiple directories [{}] found with same path'
                .format(len(dirs)))
        return dirs[0]

    def _create_tag(self, path, parent=None):
        d = omero.gateway.TagAnnotationWrapper(
            self.conn, omero.model.TagAnnotationI())
        d.setNs(self.ns)
        d.setTextValue(path, wrap=True)
        d.save()
        # OMERO_CLASS is None which causes linkAnnotation to fail
        if parent:
            parent.OMERO_CLASS = 'Annotation'
            parent.linkAnnotation(d)
        return d

    def getinfo(self, path, namespaces=None):
        """
        Get info regarding a file or directory.
        """
        path = self._normalise_path(path)
        dirname, basename = self._split_basename(path)
        d = {'basic': {'name': basename}, 'details': {}}
        dir = self._get_dir(path, throw=False)
        file = self._get_file(path, throw=False)
        if dir and file:
            raise ResourceError(
                path, msg='Directory and file found with same path')
        if not dir and not file:
            raise ResourceNotFound(path)
        if dir:
            d['basic']['is_dir'] = True
            d['details']['created'] = (
                dir.details.creationEvent.time.val / 1000)
            d['details']['size'] = 0
            d['details']['type'] = ResourceType.directory
        else:
            d['basic']['is_dir'] = False
            d['details']['created'] = (
                file.ctime or file.details.creationEvent.time.val / 1000)
            d['details']['modified'] = file.mtime / 1000
            d['details']['size'] = file.size
            d['details']['type'] = ResourceType.file
        return Info(d)

    def listdir(self, path):
        """
        Get a list of resources in a directory.
        """
        path = self._normalise_path(path)
        parent = self._get_dir(path)
        params = omero.sys.ParametersI()
        params.addId(parent.id)
        params.addString('ns', self.ns)

        rdirs = unwrap(self.conn.getQueryService().projection(
            'SELECT child.id, child.textValue FROM AnnotationAnnotationLink '
            'WHERE parent.id=:id AND child.ns=:ns',
            params))

        filelinks = list(self.conn.getAnnotationLinks(
            'OriginalFile', ann_ids=[parent.id]))
        # For files the file is the parent in the link
        return ([self._split_basename(d[1])[1] for d in rdirs] +
                [f.parent.name.val for f in filelinks])

    def makedir(self, path, permissions=None, recreate=False):
        """
        Make a directory.
        """
        path = self._normalise_path(path)
        dirname, basename = self._split_basename(path)
        d = self._get_dir(path, throw=False)
        if d:
            if not recreate:
                raise DirectoryExists(path)
        else:
            parent = self._get_dir(dirname)
            self._create_tag(path, parent)
        return SubFS(self, path)

    def openbin(self, path, mode='r', buffering=-1, **options):
        """
        Open a binary file.
        """
        path = self._normalise_path(path)
        mode = mode.replace('b', '').replace('+', '')
        dirname, basename = self._split_basename(path)
        parent = self._get_dir(dirname)
        if mode == 'r':
            f = self._get_file(path)
            fobj = OriginalFileObj(f, readonly=True)
            return fobj
        if mode in ('a', 'w'):
            f = self._get_file(path, throw=False)
            if not f:
                f = omero.gateway.OriginalFileWrapper(
                    self.conn, omero.model.OriginalFileI())
                f.setName(basename)
                f.setPath(dirname, wrap=True)
                f.save()
                f.linkAnnotation(parent)
            fobj = OriginalFileObj(f)
            if mode == 'w':
                fobj.truncate()
            fobj.seek(0, os.SEEK_END)
            return fobj
        raise Unsupported(
            path, msg='openbin mode "{}" not supported'.format(mode))

    def remove(self, path):
        """
        Remove a file.
        """
        path = self._normalise_path(path)
        f = self._get_file(path)
        self.conn.deleteObject(f._obj)

    def removedir(self, path):
        """
        Remove a directory.

        TODO: conn.deleteObject() deletes the parent directory if there are no
              other children
        BUG: The alternative method conn.deleteObjects() also deletes the
             parent even if deleteAnns=False deleteChildren=False
        """
        path = self._normalise_path(path)
        d = self._get_dir(path)
        children = self.listdir(path)
        if children:
            raise DirectoryNotEmpty(path)
        self.conn.deleteObject(d._obj)

    def setinfo(self, path, info):
        """
        Set resource information.
        """
        # path = self._normalise_path(path)
        # TODO
        raise Unsupported(path)

    def close(self):
        self.conn.close()
        super().close()
