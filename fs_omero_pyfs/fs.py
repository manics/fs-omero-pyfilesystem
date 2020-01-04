# https://docs.pyfilesystem.org/en/latest/implementers.html
from fs import (
    ResourceType,
    Seek,
)
from fs.base import FS
from fs.errors import (
    DirectoryExists,
    DirectoryExpected,
    DirectoryNotEmpty,
    FileExists,
    FileExpected,
    RemoteConnectionError,
    RemoveRootError,
    ResourceError,
    ResourceNotFound,
)
from fs.info import Info
from fs.subfs import SubFS

from io import (
    IOBase,
    UnsupportedOperation,
)
import logging
from time import time
import omero.clients
from omero.gateway import (
    BlitzGateway,
    OriginalFileWrapper,
    TagAnnotationWrapper,
)
from omero.rtypes import (
    rtime,
    unwrap,
)

DEFAULT_NS = 'github.com/manics/fs-omero-pyfs'


class OriginalFileObj(omero.gateway._OriginalFileAsFileObj, IOBase):
    # https://docs.python.org/3.6/library/io.html#io.IOBase
    # https://github.com/ome/omero-py/blob/v5.6.dev9/src/omero/gateway/__init__.py#L5293

    def __init__(self, *args, **kwargs):
        self._readable = kwargs.pop('readable', True)
        self._writable = kwargs.pop('writable', True)
        super().__init__(*args, **kwargs)

    def close(self):
        super().close()
        super(IOBase, self).close()

    def fileno(self):
        raise UnsupportedOperation('fileno not supported')

    def flush(self):
        pass

    def isatty(self):
        return False

    def seek(self, n, mode=Seek.set):
        super().seek(n, mode)
        return self.pos

    def read(self, n=-1):
        if not self._readable:
            raise PermissionError('File opened write-only')
        r = super().read(n)
        return r

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

    def truncate(self, size=None):
        if not self._writable:
            raise PermissionError('File opened read-only')
        if size is None:
            size = self.pos
        currentsize = self.rfs.size()
        currentpos = self.pos
        if size > currentsize:
            self.pos = currentsize
            self.write(b'\0' * (size - currentsize))
            self.pos = currentpos
        else:
            self.rfs.truncate(size)
        return size

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


class CachedResource:

    def __init__(self, path, resource):
        if isinstance(resource, TagAnnotationWrapper):
            self.type = ResourceType.directory
        elif isinstance(resource, OriginalFileWrapper):
            self.type = ResourceType.file
        else:
            raise ValueError('Unexpected object: {}'.format(resource))
        self.id = resource.id
        self.path = path
        self.time = time()

    def __str__(self):
        return 'CachedResource({} {}:{} {:.0f})'.format(
            self.path, self.type.name, self.id, self.time)


class OmeroFS(FS):

    # https://github.com/PyFilesystem/pyfilesystem2/blob/129567606066cd002bb45a11aae543f7b73f0134/fs/base.py#L675
    _meta = {
        'case_insensitive': False,
        'invalid_path_chars': '\0',
        'max_path_length': None,
        'max_sys_path_length': None,
        'network': True,
        'read_only': False,
        'supports_rename': True,
    }

    def __init__(self, *, host, user, passwd, root='/', create=True,
                 ns=DEFAULT_NS, groupid=None, cache_ttl=2):
        super().__init__()
        self.log = logging.getLogger(__name__)
        self.strlabel = '{}: {}@{} ns={} groupid={} cache_ttl={}'.format(
            __name__, user, host, ns, groupid, cache_ttl)
        self.ns = ns

        # Use omero.client to get a better error message if connect fails
        try:
            client = omero.client(host)
            client.setAgent('fs-omero-pyfs')
            session = client.createSession(user, passwd)
            assert session
            self.conn = BlitzGateway(client_obj=client)
            self.conn.SERVICE_OPTS.setOmeroGroup(groupid)
        except Exception as e:
            raise RemoteConnectionError(
                exc=e, msg='Failed to connect: {}'.format(self))
        self.path_cache = {}
        self.cache_ttl = cache_ttl
        self.root = root
        self.log.debug('Connected: %s', self)
        if not self._get_dir(root, throw=(not create)):
            self._create_tag(root)

    def __str__(self):
        return self.strlabel

    def _cache_put(self, path, resource):
        # self.log.debug('_cache_put(%s)', path)
        if self.cache_ttl > 0:
            self.path_cache[path] = CachedResource(path, resource)

    def _cache_get(self, path):
        # self.log.debug('_cache_get(%s)', path)
        if self.cache_ttl <= 0:
            return None
        try:
            cached = self.path_cache[path]
        except KeyError:
            return None
        if cached.time + self.cache_ttl > time():
            # self.log.debug('%s', cached)
            return cached
        self._cache_remove(path)
        return None

    def _cache_remove(self, path):
        # self.log.debug('_cache_remove(%s)', path)
        self.path_cache.pop(path, None)

    def _split_basename(self, path):
        path = self.validatepath(path)
        dirname, basename = path.rsplit('/', 1)
        return self.validatepath(dirname), basename

    def _get_file(self, path, throw=True, checkother=True):
        self.log.debug('_get_file %s %s %s', path, throw, checkother)
        vpath = self.validatepath(path)
        cached = self._cache_get(vpath)
        if cached and cached.type == ResourceType.file:
            return self.conn.getObject('OriginalFile', cached.id)
        dirname, basename = self._split_basename(path)
        if not basename:
            if throw:
                raise FileExpected(path)
        parent = self._get_dir(dirname, throw=False, checkother=False)
        if not parent:
            if throw:
                raise ResourceNotFound(path)
            return None
        params = omero.sys.ParametersI()
        params.addId(parent.id)
        params.addString('ns', self.ns)
        params.addString('filename', basename)
        files = unwrap(self.conn.getQueryService().projection(
            'SELECT parent.id FROM OriginalFileAnnotationLink '
            'WHERE parent.name=:filename '
            'AND child.id=:id '
            'AND child.ns=:ns '
            'AND child.class=TagAnnotation',
            params))
        if not files:
            if throw:
                if checkother and self._get_dir(path, throw=False):
                    raise FileExpected(path)
                raise ResourceNotFound(path)
            return None
        if len(files) > 1:
            raise ResourceError(
                path, msg='Multiple files [{}] found with same path'.format(
                    len(files)))
        file = self.conn.getObject('OriginalFile', files[0][0])
        self._cache_put(vpath, file)
        return file

    def _get_dir_ignore_parents(self, path, throw=True, checkother=True):
        vpath = self.validatepath(path)
        dirs = list(self.conn.getObjects('TagAnnotation',
                    attributes={'ns': self.ns, 'textValue': vpath}))
        if not dirs:
            if throw:
                if checkother and self._get_file(
                        path, throw=False, checkother=False):
                    raise DirectoryExpected(path)
                raise ResourceNotFound(path)
            return None
        if len(dirs) > 1:
            raise ResourceError(
                path, msg='Multiple directories [{}] found with same path'
                .format(len(dirs)))
        return dirs[0]

    def _get_dir(self, path, throw=True, checkother=True):
        self.log.debug('_get_dir %s %s %s', path, throw, checkother)
        vpath = self.validatepath(path)
        cached = self._cache_get(vpath)
        if cached and cached.type == ResourceType.directory:
            return self.conn.getObject('TagAnnotation', cached.id)
        dirname, basename = self._split_basename(path)
        if not basename:
            if dirname != self.root:
                raise ResourceError(
                    'Top-level directory "{}" != root "{}"'.format(
                        dirname, self.root))
            dir = self._get_dir_ignore_parents(
                dirname, throw=throw, checkother=checkother)
            if dir:
                self._cache_put(vpath, dir)
            return dir

        parent = self._get_dir(dirname, throw=False, checkother=False)
        if not parent:
            if throw:
                raise ResourceNotFound(path)
            return None
        params = omero.sys.ParametersI()
        params.addString('basename', basename)
        params.addString('ns', self.ns)
        params.addId(parent.id)
        dirs = unwrap(self.conn.getQueryService().projection(
            'SELECT child.id FROM AnnotationAnnotationLink '
            'WHERE parent.id=:id '
            'AND child.textValue=:basename '
            'AND child.ns=:ns '
            'AND child.class=TagAnnotation',
            params))
        if not dirs:
            if throw:
                if checkother and self._get_file(path, throw=False):
                    raise DirectoryExpected(path)
                raise ResourceNotFound(path)
            return None
        if len(dirs) > 1:
            raise ResourceError(
                path, msg='Multiple directories [{}] found with same path'
                .format(len(dirs)))
        dir = self.conn.getObject('TagAnnotation', dirs[0][0])
        self._cache_put(vpath, dir)
        return dir

    def _create_tag(self, path, parent=None):
        # path assumed to be validated
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
        vpath = self.validatepath(path)
        dirname, basename = self._split_basename(path)
        d = {'basic': {'name': basename}, 'details': {}}

        cached = self._cache_get(vpath)
        dir = None
        file = None
        if cached and cached.type == ResourceType.directory:
            dir = self._get_dir(path, throw=False, checkother=False)
        elif cached and cached.type == ResourceType.file:
            file = self._get_file(path, throw=False, checkother=False)
        else:
            dir = self._get_dir(path, throw=False, checkother=False)
            file = self._get_file(path, throw=False, checkother=False)
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
                file.ctime or file.details.creationEvent.time.val) / 1000
            d['details']['modified'] = file.mtime / 1000
            d['details']['size'] = file.size
            d['details']['type'] = ResourceType.file
        return Info(d)

    def listdir(self, path):
        """
        Get a list of resources in a directory.
        """
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
        if self._get_file(path, throw=False):
            # TODO: This should probably be FileExists, but
            # https://github.com/PyFilesystem/pyfilesystem2/blob/v2.4.11/fs/test.py#L674-L675
            # says it should be DirectoryExists
            raise DirectoryExists(path)
        vpath = self.validatepath(path)
        dirname, basename = self._split_basename(path)
        d = self._get_dir(path, throw=False)
        if d:
            if not recreate:
                raise DirectoryExists(path)
        else:
            parent = self._get_dir(dirname)
            d = self._create_tag(basename, parent)
            self._cache_put(vpath, d)
        return SubFS(self, vpath)

    def openbin(self, path, mode='r', buffering=-1, **options):
        """
        Open a binary file.
        """
        if 't' in mode:
            raise ValueError('Text mode not supported')
        mode = mode.replace('b', '')
        dirname, basename = self._split_basename(path)
        parent = self._get_dir(dirname, throw=False)
        if self._get_dir(path, throw=False):
            raise FileExpected(path)

        if not parent:
            raise ResourceNotFound(path, 'Parent directory not found')
        if 'r' in mode:
            f = self._get_file(path)
            fobj = OriginalFileObj(f, writable=('+' in mode))
            return fobj
        if 'a' in mode or 'w' in mode or 'x' in mode:
            f = self._get_file(path, throw=False)
            if f and 'x' in mode:
                raise FileExists(path)
            if not f:
                f = omero.gateway.OriginalFileWrapper(
                    self.conn, omero.model.OriginalFileI())
                f.setName(basename)
                f.setPath(dirname, wrap=True)
                f.save()
                f.linkAnnotation(parent)
            fobj = OriginalFileObj(f, readable=('+' in mode))
            if 'w' in mode:
                fobj.truncate(0)
            fobj.seek(0, Seek.end)
            return fobj
        raise ValueError(
            'openbin mode "{}" not supported: {}'.format(mode, path))

    def remove(self, path):
        """
        Remove a file.
        """
        vpath = self.validatepath(path)
        f = self._get_file(path)
        self._cache_remove(vpath)
        self.conn.deleteObject(f._obj)

    def removedir(self, path):
        """
        Remove a directory.

        TODO: conn.deleteObject() deletes the parent directory if there are no
              other children
        BUG: The alternative method conn.deleteObjects() also deletes the
             parent even if deleteAnns=False deleteChildren=False
        """
        vpath = self.validatepath(path)
        if vpath == self.root:
            raise RemoveRootError(self.root)
        d = self._get_dir(path)
        children = self.listdir(path)
        if children:
            raise DirectoryNotEmpty(path)
        self._cache_remove(vpath)
        self.conn.deleteObject(d._obj)

    def setinfo(self, path, info):
        """
        Set resource information.
        Only supports ctime and mtime for files
        """
        mtime = info.get('details', {}).get('modified')
        ctime = info.get('details', {}).get('created')
        f = self._get_file(path)
        if mtime:
            f._obj.mtime = rtime(mtime * 1000)
        if ctime:
            f._obj.ctime = rtime(ctime * 1000)
        f.save()

    def close(self):
        self.conn.close()
        super().close()
