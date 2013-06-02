# A port of the NFSv2 XDR specification to Ruby XDR/SUNRPC. Based on RFC 1094.
#
# Author: Brian Ollenberger

require 'sunrpc'

module NFS

include SUNRPC

PORT       = 2049
MAXDATA    = 8192
MAXPATHLEN = 1024
MAXNAMELEN = 255
FHSIZE     = 32
FIFO_DEV   = -1 # size kludge for named pipes

MODE_FMT  = 0170000 # type of file
MODE_DIR  = 0040000 # directory
MODE_CHR  = 0020000 # character special
MODE_BLK  = 0060000 # block special
MODE_REG  = 0100000 # regular
MODE_LNK  = 0120000 # symbolic link
MODE_SOCK = 0140000 # socket
MODE_FIFO = 0010000 # fifo

Nfsstat = Enumeration.new do
	name :NFS_OK, 0              # no error
	name :NFSERR_PERM, 1         # Not owner
	name :NFSERR_NOENT, 2        # No such file or directory
	name :NFSERR_IO, 5           # I/O error
	name :NFSERR_NXIO, 6         # No such device or address
	name :NFSERR_ACCES, 13       # Permission denied
	name :NFSERR_EXIST, 17       # File exists
	name :NFSERR_NODEV, 19       # No such device
	name :NFSERR_NOTDIR, 20      # Not a directory
	name :NFSERR_ISDIR, 21       # Is a directory
	name :NFSERR_INVAL, 22       # Invalid argument
	name :NFSERR_FBIG, 27        # File too large
	name :NFSERR_NOSPC, 28       # No space left on device
	name :NFSERR_ROFS, 30        # Read-only file system
	name :NFSERR_NAMETOOLONG, 63 # File name too long
	name :NFSERR_NOTEMPTY, 66    # Directory not empty
	name :NFSERR_DQUOT, 69       # Disc quota exceeded
	name :NFSERR_STALE, 70       # Stale NFS file handle
	name :NFSERR_WFLUSH, 99      # Write cache flushed
end

ftype = Enumeration.new do
	name :NFNON, 0  # non-file
	name :NFREG, 1  # regular file
	name :NFDIR, 2  # directory
	name :NFBLK, 3  # block special
	name :NFCHR, 4  # character special
	name :NFLNK, 5  # symbolic link
	name :NFSOCK, 6 # unix domain sockets
	name :NFBAD, 7  # unused
	name :NFFIFO, 8 # named pipe
end

Nfs_fh = Structure.new do
	component :data, FixedOpaque.new(FHSIZE)
end

nfstime = Structure.new do
	component :seconds, UnsignedInteger.new
	component :useconds, UnsignedInteger.new
end

fattr = Structure.new do
	component :type, ftype                    # file type
	component :mode, UnsignedInteger.new      # protection mode bits
	component :nlink, UnsignedInteger.new     # number of hard links
	component :uid, UnsignedInteger.new       # owner user id
	component :gid, UnsignedInteger.new       # owner group id
	component :size, UnsignedInteger.new      # file size in bytes
	component :blocksize, UnsignedInteger.new # prefered block size
	component :rdev, UnsignedInteger.new      # special device number
	component :blocks, UnsignedInteger.new    # Kb of disk used by file
	component :fsid, UnsignedInteger.new      # device number
	component :fileid, UnsignedInteger.new    # inode number
	component :atime, nfstime # time of last access
	component :mtime, nfstime # time of last modification
	component :ctime, nfstime # time of last change
end

sattr = Structure.new do
	component :mode, UnsignedInteger.new # protection mode bits
	component :uid, UnsignedInteger.new  # owner user id
	component :gid, UnsignedInteger.new  # owner group id
	component :size, UnsignedInteger.new # file size in bytes
	component :atime, nfstime            # time of last access
	component :mtime, nfstime            # time of last modification
end

Filename = String.new(MAXNAMELEN)
nfspath = String.new(MAXPATHLEN)

attrstat = Union.new(Nfsstat) do
	arm :NFS_OK do
		component :attributes, fattr
	end
	
	default do
	end
end

sattrargs = Structure.new do
	component :file, Nfs_fh
	component :attributes, sattr
end

diropargs = Structure.new do
	component :dir, Nfs_fh
	component :name, Filename
end

diropokres = Structure.new do
	component :file, Nfs_fh
	component :attributes, fattr
end

diropres = Union.new(Nfsstat) do
	arm :NFS_OK do
		component :diropres, diropokres
	end
	
	default do
	end
end

readlinkres = Union.new(Nfsstat) do
	arm :NFS_OK do
		component :data, nfspath
	end
	
	default do
	end
end

# Arguments to remote read
readargs = Structure.new do
	component :file, Nfs_fh                       # handle for file
	component :offset, UnsignedInteger.new     # byte offset in file
	component :count, UnsignedInteger.new      # immediate read count
	component :totalcount, UnsignedInteger.new # read count from offset
end

# Status OK portion of remote read reply
readokres = Structure.new do
	component :attributes, fattr # Attributes needed for pagin ??
	component :data, Opaque.new(MAXDATA)
end

readres = Union.new(Nfsstat) do
	arm :NFS_OK do
		component :reply, readokres
	end
	
	default do
	end
end

# Arguments to remote write
writeargs = Structure.new do
	component :file, Nfs_fh                     # handle for file
	component :beginoffset, UnsignedInteger.new # begin. byte offset in file
	component :offset, UnsignedInteger.new      # curr. byte offset in file
	component :totalcount, UnsignedInteger.new  # write count to this offset
	component :data, Opaque.new(MAXDATA)        # data
end

createargs = Structure.new do
	component :where, diropargs
	component :attributes, sattr
end

renameargs = Structure.new do
	component :from, diropargs
	component :to, diropargs
end

linkargs = Structure.new do
	component :from, Nfs_fh
	component :to, diropargs
end

symlinkargs = Structure.new do
	component :from, diropargs
	component :to, nfspath
	component :attributes, sattr
end

nfscookie = UnsignedInteger.new

# Arguments to readdir
readdirargs = Structure.new do
	component :dir, Nfs_fh                   # directory handle
	component :cookie, nfscookie             # cookie
	component :count, UnsignedInteger.new # directory bytes to read
end

entry = Structure.new do
	component :fileid, UnsignedInteger.new
	component :name, Filename
	component :cookie, nfscookie
	component :nextentry, Optional.new(self)
end

dirlist = Structure.new do
	component :entries, Optional.new(entry)
	component :eof, Boolean.new
end

readdirres = Union.new(Nfsstat) do
	arm :NFS_OK do
		component :reply, dirlist
	end
end

statfsokres = Structure.new do
	component :tsize, UnsignedInteger.new  # preferred xfer size in bytes
	component :bsize, UnsignedInteger.new  # file system block size
	component :blocks, UnsignedInteger.new # total blocks in file system
	component :bfree, UnsignedInteger.new  # free blocks in fs
	component :bavail, UnsignedInteger.new # free blocks avail to non-root
end

statfsres = Union.new(Nfsstat) do
	arm :NFS_OK do
		component :reply, statfsokres
	end
	
	default do
	end
end

# Remote file service routines
NFS_VERSION = 2

NFS_PROGRAM = Program.new 100003 do
	version NFS_VERSION do
		procedure attrstat, :GETATTR, 1, Nfs_fh
		procedure attrstat, :SETATTR, 2, sattrargs
		procedure Void.new, :ROOT, 3, Void.new
		procedure diropres, :LOOKUP, 4, diropargs
		procedure readlinkres, :READLINK, 5, Nfs_fh
		procedure readres, :READ, 6, readargs
		procedure Void.new, :WRITECACHE, 7, Void.new
		procedure attrstat, :WRITE, 8, writeargs
		procedure diropres, :CREATE, 9, createargs
		procedure Nfsstat, :REMOVE, 10, diropargs
		procedure Nfsstat, :RENAME, 11, renameargs
		procedure Nfsstat, :LINK, 12, linkargs
		procedure Nfsstat, :SYMLINK, 13, symlinkargs
		procedure diropres, :MKDIR, 14, createargs
		procedure Nfsstat, :RMDIR, 15, diropargs
		procedure readdirres, :READDIR, 16, readdirargs
		procedure statfsres, :STATFS, 17, Nfs_fh
	end
end

end
