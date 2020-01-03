"""Defines the OmeroFSOpener."""

__all__ = ['OmeroFSOpener']

from fs.opener import Opener

from .fs import OmeroFS


class OmeroFSOpener(Opener):
    protocols = [
        'omero',
        'omero+ws',
        'omero+wss',
    ]

    def open_fs(self, fs_url, parse_result, writeable, create, cwd):
        # https://docs.pyfilesystem.org/en/latest/reference/opener.html#fs.opener.parse.ParseResult
        _, _, subprotocol = parse_result.protocol.partition('+')
        if subprotocol:
            host = '{}://{}'.format(subprotocol, parse_result.resource)
        else:
            host = parse_result.resource
        omeroargs = {
            'host': host,
            'user': parse_result.username,
            'passwd': parse_result.password,
            'create': create,
        }
        ns = parse_result.params.get('ns')
        if ns:
            omeroargs['ns'] = ns
        group = parse_result.params.get("group")
        if group:
            omeroargs['group'] = group
        cache_ttl = parse_result.params.get("cachettl")
        if cache_ttl:
            omeroargs['cache_ttl'] = int(cache_ttl)
        omerofs = OmeroFS(**omeroargs)
        return omerofs
