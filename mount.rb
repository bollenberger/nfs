# The XDR representation of the NFS mount protocol. Based on RFC 1094.
#
# Author: Brian Ollenberger

require 'sunrpc'
require 'nfs'

module Mount

include SUNRPC

MNTPATHLEN = 1024 # maximum bytes in a pathname argument
MNTNAMLEN = 255   # maximum bytes in a name argument

# The fhandle is the file handle that the server passes to the client.
# All file operations are done using the file handles to refer to a file
# or a directory. The file handle can contain whatever information the
# server needs to distinguish an individual file.
# -just use nfs_fh from the nfs protocol.

# If a status of zero is returned, the call completed successfully, and
# a file handle for the directory follows. A non-zero status indicates
# some sort of error. The status corresponds with UNIX error numbers.
fhstatus = Union.new(NFS::Nfsstat) do
	arm :NFS_OK do
		component :fhs_fhandle, NFS::Nfs_fh
	end
	
	default do
	end
end

# The type dirpath is the pathname of a directory
dirpath = String.new(MNTPATHLEN)

# The type name is used for arbitrary names (hostnames, groupnames)
name = String.new(MNTNAMLEN)

# A list of who has what mounted
mountbody = Structure.new do
	component :ml_hostname, name
	component :ml_directory, dirpath
	component :ml_next, Optional.new(self)
end
mountlist = Optional.new(mountbody)

# A list of netgroups
groupnode = Structure.new do
	component :gr_name, name
	component :gr_next, Optional.new(self)
end
groups = Optional.new(groupnode)

# A list of what is exported and to whom
exportnode = Structure.new do
	component :ex_dir, dirpath
	component :ex_groups, groups
	component :ex_next, Optional.new(self)
end
exports = Optional.new(exportnode)

MOUNTVERS = 1
MOUNTPROG = Program.new 100005 do
	version MOUNTVERS do
		# If fhs_status is 0, then fhs_fhandle contains the
        # file handle for the directory. This file handle may
        # be used in the NFS protocol. This procedure also adds
        # a new entry to the mount list for this client mounting
        # the directory.
        # Unix authentication required.
		procedure fhstatus, :MNT, 1, dirpath
		
		# Returns the list of remotely mounted filesystems. The
        # mountlist contains one entry for each hostname and
        # directory pair.
		procedure mountlist, :DUMP, 2, Void.new
		
		# Removes the mount list entry for the directory
        # Unix authentication required.
		procedure Void.new, :UMNT, 3, dirpath
		
		# Removes all of the mount list entries for this client
        # Unix authentication required.
		procedure Void.new, :UMNTALL, 4, Void.new
		
		# Returns a list of all the exported filesystems, and which
        # machines are allowed to import it.
		procedure exports, :EXPORT, 5, Void.new
		
		# Identical to MOUNTPROC_EXPORT above
		procedure exports, :EXPORTALL, 6, Void.new
	end
end

end
