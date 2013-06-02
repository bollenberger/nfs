# An NFS server.
#
# Author: Brian Ollenberger

require 'nfs'
require 'mount'
require 'vcfs'

class VCFSServer
	def initialize(vcfs_params, fsid = 0)
		@mount_prog = Mount::MOUNTPROG.dup
		@mount_vers = Mount::MOUNTVERS
		@nfs_prog = NFS::NFS_PROGRAM.dup
		@nfs_vers = NFS::NFS_VERSION
		
		if not vcfs_params.kind_of?(Array)
			vcfs_params = [vcfs_params]
		end
		# TODO pool-ize the VCFS so that we can do things concurrently
		@vcfs = VCFS::VCFS.new(*vcfs_params)
		@fsid = fsid
		
		define_mount_procedures
		define_nfs_procedures
	end
	
	def auth_unix(auth)
		if auth[:flavor] != :AUTH_UNIX
			raise Errno::EACCES
		end
		
		SUNRPC::Auth_unix.decode(auth[:body])
	end
	
	def programs
		[@mount_prog, @nfs_prog]
	end
	
	def to_fh(branch, file)
		[branch, file, ''].pack('N2a24')
	end
	
	def from_fh(fh)
		branch, file = fh.unpack('N2a24')
		if branch == 0 and file == 0
			[1, 1]
		else
			[branch, file]
		end
	end
	
	def handle_errors
		begin
			yield
		rescue Errno::EPERM
			{:_discriminant => :NFSERR_PERM}
		rescue Errno::ENOENT
			{:_discriminant => :NFSERR_NOENT}
		rescue Errno::EIO
			{:_discriminant => :NFSERR_IO}
		rescue Errno::ENXIO
			{:_discriminant => :NFSERR_NXIO}
		rescue Errno::EACCES
			{:_discriminant => :NFSERR_ACCES}
		rescue Errno::EEXIST
			{:_discriminant => :NFSERR_EXIST}
		rescue Errno::ENODEV
			{:_discriminant => :NFSERR_NODEV}
		rescue Errno::ENOTDIR
			{:_discriminant => :NFSERR_NOTDIR}
		rescue Errno::EISDIR
			{:_discriminant => :NFSERR_ISDIR}
		rescue Errno::EINVAL
			{:_discriminant => :NFSERR_INVAL}
		rescue Errno::EFBIG
			{:_discriminant => :NFSERR_FBIG}
		rescue Errno::ENOSPC
			{:_discriminant => :NFSERR_NOSPC}
		rescue Errno::EROFS
			{:_discriminant => :NFSERR_ROFS}
		rescue Errno::ENAMETOOLONG
			{:_discriminant => :NFSERR_NAMETOOLONG}
		rescue Errno::ENOTEMPTY
			{:_discriminant => :NFSERR_NOTEMPTY}
		rescue Errno::EDQUOT
			{:_discriminant => :NFSERR_DQUOT}
		rescue Errno::ESTALE
			{:_discriminant => :NFSERR_STALE}
		rescue => e
			# LOG
			$stderr.puts e
			$stderr.print e.backtrace.join("\n")
			{:_discriminant => :NFSERR_IO}
		end
	end
	
	def define_mount_procedures
		@mount_prog.on_call(@mount_vers, :MNT) do |arg, auth, verf|
			puts 'MNT'
			puts arg.inspect
			
			if arg == '/'
				{
					:_discriminant => :NFS_OK,
					:fhs_fhandle => {
						:data => to_fh(1, 1)
					}
				}
			else
				{:_discriminant => :NFSERR_ACCES}
			end
		end
		
		@mount_prog.on_call(@mount_vers, :DUMP) do |arg, auth, verf|
			puts 'DUMP'
			puts arg.inspect
			
			nil
		end
		
		@mount_prog.on_call(@mount_vers, :UMNT) do |arg, auth, verf|
			puts 'UMNT'
			puts arg.inspect
			# do nothing
		end
		
		@mount_prog.on_call(@mount_vers, :UMNTALL) do |arg, auth, verf|
			puts 'UMNTALL'
			puts arg.inspect
			# do nothing
		end
		
		export = proc do |arg, auth, verf|
			puts 'EXPORT'
			puts arg.inspect
			
			{
				:ex_dir => '/',
				:ex_groups => nil,
				:ex_next => nil
			}
		end
		@mount_prog.on_call(@mount_vers, :EXPORT, &export)
		@mount_prog.on_call(@mount_vers, :EXPORTALL, &export)
	end
	
	# Convert Ruby File::Stat(-like) object to an NFS fattr
	def convert_attrs(attrs)
		type = :NFNON
		mode = attrs.mode
		if attrs.file?
			type = :NFREG
			mode |= NFS::MODE_REG
		elsif attrs.directory?
			type = :NFDIR
			mode |= NFS::MODE_DIR
		elsif attrs.blockdev?
			type = :NFBLK
			mode |= NFS::MODE_BLK
		elsif attrs.chardev?
			type = :NFCHR
			mode |= NFS::MODE_CHR
		elsif attrs.symlink?
			type = :NFLNK
			mode |= NFS::MODE_LNK
		elsif attrs.socket?
			type = :NFSOCK
			mode |= NFS::MODE_SOCK
		end
		
		{
			:type => type,
			:mode => mode,
			:nlink => attrs.nlink,
			:uid => attrs.uid,
			:gid => attrs.gid,
			:size => attrs.size,
			:blocksize => attrs.blksize,
			:rdev => attrs.rdev,
			:blocks => attrs.blocks,
			:fsid => @fsid,
			:fileid => attrs.ino,
			:atime => {
				:seconds => attrs.atime.tv_sec,
				:useconds => attrs.atime.tv_usec
			},
			:mtime => {
				:seconds => attrs.mtime.tv_sec,
				:useconds => attrs.mtime.tv_usec
			},
			:ctime => {
				:seconds => attrs.ctime.tv_sec,
				:useconds => attrs.ctime.tv_usec
			}
		}
	end
	
	def define_nfs_procedures
		@nfs_prog.on_call(@nfs_vers, :GETATTR) do |arg, auth, verf|
			puts 'GETATTR'
			puts arg.inspect
			
			handle_errors do
				attrs = @vcfs.getattr(*from_fh(arg[:data]))
				{
					:_discriminant => :NFS_OK,
					:attributes => convert_attrs(attrs)
				}
			end
		end
		
		@nfs_prog.on_call(@nfs_vers, :SETATTR) do |arg, auth, verf|
			puts 'SETATTR'
			puts arg.inspect
			
			handle_errors do
				# Get -1 represented as an unsigned integer. The sattr fields
				# are -1 to represent that they should not be changed.
				neg_one = 4294967295
				
				if arg[:attributes][:mode] == neg_one
					arg[:attributes][:mode] = nil
				end
				if arg[:attributes][:uid] == neg_one
					arg[:attributes][:uid] = nil
				end
				if arg[:attributes][:gid] == neg_one
					arg[:attributes][:gid] = nil
				end
				if arg[:attributes][:size] == neg_one
					arg[:attributes][:size] = nil
				end
				if arg[:attributes][:atime][:seconds] == neg_one
					arg[:attributes][:atime] = nil
				else
					arg[:attributes][:atime] = Time.at(
						arg[:attributes][:atime][:seconds],
						arg[:attributes][:atime][:useconds])
				end
				if arg[:attributes][:mtime][:seconds] == neg_one
					arg[:attributes][:mtime] = nil
				else
					arg[:attributes][:mtime] = Time.at(
						arg[:attributes][:mtime][:seconds],
						arg[:attributes][:mtime][:useconds])
				end
				
				branch, file = from_fh(arg[:file][:data])
				attrs = @vcfs.setattr(branch, file,
					arg[:attributes][:mode],
					arg[:attributes][:uid],
					arg[:attributes][:gid],
					arg[:attributes][:size],
					arg[:attributes][:atime],
					arg[:attributes][:mtime])
				
				{
					:_discriminant => :NFS_OK,
					:attributes => convert_attrs(attrs)
				}
			end
		end
		
		@nfs_prog.on_call(@nfs_vers, :ROOT) do |arg, auth, verf|
			puts 'ROOT'
			puts arg.inspect
			# obsolete
		end
		
		@nfs_prog.on_call(@nfs_vers, :LOOKUP) do |arg, auth, verf|
			puts 'LOOKUP'
			puts arg.inspect
			
			handle_errors do
				branch, dir = from_fh(arg[:dir][:data])
				file = @vcfs.lookup(branch, dir, arg[:name])
				attrs = @vcfs.getattr(branch, file)

				result = {
					:_discriminant => :NFS_OK,
					:diropres => {
						:file => {
							:data => to_fh(branch, file)
						},
						:attributes => convert_attrs(attrs)
					}
				}

				result
			end
		end
		
		@nfs_prog.on_call(@nfs_vers, :READLINK) do |arg, auth, verf|
			puts 'READLINK'
			puts arg.inspect
			
			handle_errors do
				branch, file = from_fh(arg[:data])
				
				from = 0
				length = 1024
				result = ''
				s = @vcfs.read(branch, file, from, length)
				puts s
				while s.size > 0
					result += s
					from += length
					if s.size < length
						break
					end
					s = @vcfs.read(branch, file, from, length)
				end
				puts result
				{
					:_discriminant => :NFS_OK,
					:data => result
				}
			end
		end
		
		@nfs_prog.on_call(@nfs_vers, :READ) do |arg, auth, verf|
			puts 'READ'
			puts arg.inspect
			
			handle_errors do
				branch, file = from_fh(arg[:file][:data])
				result = @vcfs.read(branch, file, arg[:offset], arg[:count])
				attrs = @vcfs.getattr(branch, file)

				{
					:_discriminant => :NFS_OK,
					:reply => {
						:attributes => convert_attrs(attrs),
						:data => result
					}
				}
			end
		end
		
		@nfs_prog.on_call(@nfs_vers, :WRITECACHE) do |arg, auth, verf|
			puts 'WRITECACHE'
			puts arg.inspect
			
			# do nothing
		end
		
		@nfs_prog.on_call(@nfs_vers, :WRITE) do |arg, auth, verf|
			puts 'WRITE'
			#puts arg.inspect
			
			handle_errors do
				branch, file = from_fh(arg[:file][:data])
				@vcfs.write(branch, file, arg[:offset], arg[:data])
				attrs = @vcfs.getattr(branch, file)
				
				{
					:_discriminant => :NFS_OK,
					:attributes => convert_attrs(attrs)
				}
			end
		end
		
		@nfs_prog.on_call(@nfs_vers, :CREATE) do |arg, auth, verf|
			puts 'CREATE'
			puts arg.inspect
			
			handle_errors do
				auth = auth_unix(auth)
				
				neg_one = 4294967295
				
				uid = arg[:attributes][:uid]
				if uid == neg_one
					uid = auth[:uid]
				end
				gid = arg[:attributes][:gid]
				if gid == neg_one
					gid = auth[:gid]
				end
				
				branch, dir = from_fh(arg[:where][:dir][:data])
				file = @vcfs.mkfile(branch, dir, arg[:where][:name],
					arg[:attributes][:mode], uid, gid)
				attrs = @vcfs.getattr(branch, file)
				
				{
					:_discriminant => :NFS_OK,
					:diropres => {
						:file => {
							:data => to_fh(branch, file)
						},
						:attributes => convert_attrs(attrs)
					}
				}
			end
		end
		
		@nfs_prog.on_call(@nfs_vers, :REMOVE) do |arg, auth, verf|
			puts 'REMOVE'
			puts arg.inspect
			
			(handle_errors do
				branch, dir = from_fh(arg[:dir][:data])
				@vcfs.unlink(branch, dir, arg[:name])
				
				{:_discriminant => :NFS_OK}
			end)[:_discriminant]
		end
		
		@nfs_prog.on_call(@nfs_vers, :RENAME) do |arg, auth, verf|
			puts 'RENAME'
			puts arg.inspect
			
			(handle_errors do
				branch, from_dir = from_fh(arg[:from][:dir][:data])
				to_branch, to_dir = from_fh(arg[:to][:dir][:data])
				from_name = arg[:from][:name]
				to_name = arg[:to][:name]
				
				if branch != to_branch
					raise Errno::EIO
				end

				@vcfs.rename(branch, from_dir, from_name, to_dir, to_name)
				
				{:_discriminant => :NFS_OK}
			end)[:_discriminant]
		end
		
		@nfs_prog.on_call(@nfs_vers, :LINK) do |arg, auth, verf|
			puts 'LINK'
			puts arg.inspect
			
			(handle_errors do
				branch, from = from_fh(arg[:from][:data])
				to_branch, to_dir = from_fh(arg[:to][:dir][:data])
				to_name = arg[:to][:name]
				
				if branch != to_branch
					raise Errno::EIO
				end

				@vcfs.link(branch, from, to_dir, to_name)
				
				{:_discriminant => :NFS_OK}
			end)[:_discriminant]
		end
		
		@nfs_prog.on_call(@nfs_vers, :SYMLINK) do |arg, auth, verf|
			puts 'SYMLINK'
			puts arg.inspect
			
			(handle_errors do
				branch, dir = from_fh(arg[:from][:dir][:data])
				name = arg[:from][:name]
				to_name = arg[:to]
				attrs = arg[:attributes]
				
				auth = auth_unix(auth)
				neg_one = 4294967295
				
				uid = attrs[:uid]
				if uid == neg_one
					uid = auth[:uid]
				end
				gid = attrs[:gid]
				if gid == neg_one
					gid = auth[:gid]
				end
				
				puts to_name
				@vcfs.symlink(branch, dir, name, to_name,
					attrs[:mode], uid, gid)

				{:_discriminant => :NFS_OK}
			end)[:_discriminant]
		end
		
		@nfs_prog.on_call(@nfs_vers, :MKDIR) do |arg, auth, verf|
			puts 'MKDIR'
			puts arg.inspect
			
			handle_errors do
				auth = auth_unix(auth)
				
				branch, dir = from_fh(arg[:where][:dir][:data])
				
				neg_one = 4294967295
				
				uid = arg[:attributes][:uid]
				if uid == neg_one
					uid = auth[:uid]
				end
				gid = arg[:attributes][:gid]
				if gid == neg_one
					gid = auth[:gid]
				end
				
				newdir = @vcfs.mkdir(branch, dir, arg[:where][:name],
					arg[:attributes][:mode], uid, gid)
				attrs = @vcfs.getattr(branch, newdir)

				{
					:_discriminant => :NFS_OK,
					:diropres => {
						:file => {
							:data => to_fh(branch, newdir)
						},
						:attributes => convert_attrs(attrs)
					}
				}
			end
		end
		
		@nfs_prog.on_call(@nfs_vers, :RMDIR) do |arg, auth, verf|
			puts 'RMDIR'
			puts arg.inspect
			
			(handle_errors do
				branch, dir = from_fh(arg[:dir][:data])
				@vcfs.rmdir(branch, dir, arg[:name])

				{:_discriminant => :NFS_OK}
			end)[:_discriminant]
		end
		
		@nfs_prog.on_call(@nfs_vers, :READDIR) do |arg, auth, verf|
			puts 'READDIR'
			puts arg.inspect
			
			handle_errors do
				branch, dir = from_fh(arg[:dir][:data])

				cookie = arg[:cookie]
				count = arg[:count]

				need_bytes = 16 + 12

				entries = @vcfs.readdir(branch, dir)

				result_entries = nil
				last_entry = nil

				while cookie < entries.size and need_bytes < count
					need_bytes += NFS::Filename.encode(entries[cookie]).size
					
					next_entry = {
						:fileid => 1,
						:name => entries[cookie],
						:cookie => cookie,
						:nextentry => nil
					}

					if not last_entry.nil?
						last_entry[:nextentry] = next_entry
						last_entry = next_entry
					end

					if result_entries.nil?
						result_entries = next_entry
						last_entry = next_entry
					end

					cookie += 1
					need_bytes += 16
				end

				eof = :TRUE
				if need_bytes > count
					eof = :FALSE
				end

				{
					:_discriminant => :NFS_OK,
					:reply => {
						:entries => result_entries,
						:eof => eof
					}
				}
			end
		end
		
		@nfs_prog.on_call(@nfs_vers, :STATFS) do |arg, auth, verf|
			puts 'STATFS'
			puts arg.inspect
			
			handle_errors do
				{
					:_discriminant => :NFS_OK,
					:reply => {
						:tsize => 1024,
						:bsize => 1024,
						:blocks => 100,
						:bfree => 100,
						:bavail => 100
					}
				}
			end
		end
	end
	
	attr_reader :root
end
