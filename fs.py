# https://docs.pyfilesystem.org/en/latest/implementers.html
from fs.base import FS
from fs.enums import ResourceType
from fs.errors import (
    DirectoryExists,
    DirectoryNotEmpty,
    ResourceError,
    ResourceNotFound,
    Unsupported,
)
from fs.info import Info
from fs.subfs import SubFS

import os
import omero.clients
from omero.gateway import BlitzGateway
from omero.rtypes import unwrap

NS = 'github.com/manics/fs-omero-pyfilesystem'


conn = BlitzGateway(
    host=os.getenv('OMERO_HOST'),
    username=os.getenv('OMERO_SESSION') or os.getenv('OMERO_USER'),
    passwd=os.getenv('OMERO_SESSION') or os.getenv('OMERO_PASSWORD'),
    secure=True,
    useragent='fs-omero-pyfilesystem',
)
conn.c.enableKeepAlive(60)
connected = conn.connect()
assert connected
print(connected)


class OriginalFileObj(omero.gateway._OriginalFileAsFileObj):

    def __init__(self, *args, **kwargs):
        self.readonly = kwargs.pop('readonly', False)
        super().__init__(*args, **kwargs)

    def truncate(self, size=0):
        if self.readonly:
            raise PermissionError('File opened read-only')
        self.rfs.truncate(size)

    def write(self, buf):
        if self.readonly:
            raise PermissionError('File opened read-only')
        n = len(buf)
        self.rfs.write(buf, self.pos, n)
        self.pos += n


class OmeroFS(FS):

    def __init__(self, conn, root='/', create=True):
        super().__init__()
        self.conn = conn
        self.root = self._get_dir(root, throw=(not create))
        if not self.root:
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
        files = list(self.conn.getObjects('OriginalFile',
                     attributes={'path': dirname, 'name': basename}))
        if not files:
            if throw:
                raise ResourceNotFound(path)
            return None
        if len(files) > 1:
            raise ResourceError(
                path, msg='Multiple files [{}] found with same path'.format(
                    len(files)))
        return files[0]

    def _get_dir(self, path, throw=True):
        dirs = list(self.conn.getObjects('TagAnnotation',
                    attributes={'ns': NS, 'textValue': path}))
        if not dirs:
            if throw:
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
        d.setNs(NS)
        d.setTextValue(path, wrap=True)
        print('Creating directory', path, 'parent', parent)
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
                dir[0].details.creationEvent.time.val / 1000)
            d['details']['size'] = 0
            d['details']['type'] = ResourceType.directory
        else:
            d['basic']['is_dir'] = False
            d['details']['created'] = (
                file.ctime or file.details.creationEvent.time.val / 1000)
            d['details']['modified'] = file.mtime
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
        params.addString('ns', NS)

        rdirs = unwrap(conn.getQueryService().projection(
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
        mode = mode.replace('b', '')
        dirname, basename = self._split_basename(path)
        d = self._get_dir(dirname)
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
                f.setPath(dirname)
                f.linkAnnotation(d)
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
        print('Deleting directory {}'.format(path))
        self.conn.deleteObject(d._obj)

    def setinfo(self, path, info):
        """
        Set resource information.
        """
        path = self._normalise_path(path)
        # TODO
        raise Unsupported(path)
