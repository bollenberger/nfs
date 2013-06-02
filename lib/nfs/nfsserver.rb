# An NFS server.
#
# Author: Brian Ollenberger

require 'nfs'
require 'mount'

class File
	def File.new(*args, &block)
		super(*args, &block)._nfs_setup
	end
	
	def File.open(*args)
		f = super(*args)._nfs_setup
		if block_given?
			begin
				return yield(f)
			ensure
				f.close
			end
		end
		return f
	end
	
	def _nfs_setup
		@absolute_path = File.expand_path(path)
		@looked_up = {}
		self
	end
	
	def create(name, mode, uid, gid)
		f = nil
		begin
			f = File.new(_lookup(name),
				File::RDWR|File::CREAT, mode)
		rescue
			f = File.new(_lookup(name),
				File::RDONLY|File::CREAT, mode)
		end
		#f.chown(uid, gid)
		
		stat = f.lstat
		
		@looked_up[[stat.ino, name]] = f
		
		return f, stat
	end
	
	def _lookup(name)
		File.expand_path(name, @absolute_path)
	end
	
	def lookup(name)
		f = nil
		begin
			f = File.new(_lookup(name),
				File::RDWR)
		rescue
			f = File.new(_lookup(name), File::RDONLY)
		end
		
		stat = f.lstat
		
		key = [stat.ino, name]
		if @looked_up.has_key?(key)
			@looked_up[key]
		else
			@looked_up[key] = f
		end
	end
	
	def delete(name)
		File.delete(_lookup(name))
	end
	
	def rename(from_name, to_dir, to_name)
		File.rename(_lookup(from_name), to_dir._lookup(to_name))
	end
	
	def link(dir, name)
		File.link(@absolute_path, dir._lookup(name))
	end
	
	def symlink(name, to_name)
		File.symlink(to_name, _lookup(name))
	end
	
	def readlink
		File.readlink(@absolute_path)
	end
	
	def mkdir(name, mode, uid, gid)
		path = _lookup(name)
		Dir.mkdir(path, mode)
		
		f = File.new(path)
		#f.chown(uid, gid)
		
		stat = f.lstat
		@looked_up[[stat.ino, name]] = f
		
		return f, stat
	end
	
	def rmdir(name)
		Dir.delete(_lookup(name))
	end
	
	def unlink(name)
		File.unlink(_lookup(name))
	end
	
	def entries
		Dir.entries(@absolute_path)
	end
	
	def utime(atime, mtime)
		File.utime(atime, mtime, @absolute_path)
	end
end

class Filehandle < String
	def initialize
		super("\0" * NFS::FHSIZE)
	end
	
	def increment!
		size.times do |i|
			self[i] += 1
			if self[i] != 0
				return self
			end
		end
		self
	end
end

class NFSServer	
	def initialize(root = nil, fsid = 0)
		@mount_prog = Mount::MOUNTPROG.dup
		@mount_vers = Mount::MOUNTVERS
		@nfs_prog = NFS::NFS_PROGRAM.dup
		@nfs_vers = NFS::NFS_VERSION
		
		@exports = {}
		@fh_table = {}
		@file_objects = {}
		@next_fh = Filehandle.new
		
		@fsid = fsid
		
		if not root.nil?
			export('/', root)
		end
		
		define_mount_procedures
		define_nfs_procedures
		
		if block_given?
			instance_eval(&block)
		end
	end
	
	def programs
		[@mount_prog, @nfs_prog]
	end
	
	def export(path, file)
		@exports[path] = add_filehandle(file)
	end
	
	def add_filehandle(file)
		if @file_objects.has_key?(file)
			@file_objects[file]
		else
			fh = @next_fh.dup
			@fh_table[fh] = file
			@file_objects[file] = fh
			@next_fh.increment!
			fh
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
			
			if @exports.has_key?(arg)
				{
					:_discriminant => :NFS_OK,
					:fhs_fhandle => {
						:data => @exports[arg]
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
			
			result = nil
			@exports.each_key do |name|
				result = {
					:ex_dir => name,
					:ex_groups => nil,
					:ex_next => result
				}
			end
			result
		end
		@mount_prog.on_call(@mount_vers, :EXPORT, &export)
		@mount_prog.on_call(@mount_vers, :EXPORTALL, &export)
	end
	
	# Convert Ruby Stat object to an NFS fattr
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
				attrs = @fh_table[arg[:data]].lstat
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
				f = @fh_table[arg[:file][:data]]
				attrs = convert_attrs(f.lstat)

				# Get -1 represented as an unsigned integer. The sattr fields
				# are -1 to represent that they should not be changed.
				neg_one = 4294967295

				# Start with the mode. Setattr won't change the type of a file
				# and apparently some NFS clients don't set the type, so mask
				# that part out to keep what we have already.
				if arg[:attributes][:mode] != neg_one
					attrs[:mode] &= ~07777
					attrs[:mode] |= 07777 & arg[:attributes][:mode]
					
					f.chmod(arg[:attributes][:mode] & 07777)
				end
				
				# Next do the UID and GID
				if arg[:attributes][:uid] != neg_one or
					arg[:attributes][:gid] != neg_one

					uid = arg[:attributes][:uid]
					gid = arg[:attributes][:gid]

					if uid == neg_one
						uid = attrs[:uid]
					end
					if gid == neg_one
						gid = attrs[:gid]
					end
					attrs[:uid] = uid
					attrs[:gid] = gid
					
					f.chown(uid, gid)
				end
				
				# Set size (truncate)
				if arg[:attributes][:size] != neg_one
					attrs[:size] = arg[:attributes][:size]
					f.truncate(arg[:attributes][:size])
				end
				
				# Set time
				if arg[:attributes][:atime][:seconds] != neg_one or
					arg[:attributes][:mtime][:seconds] != neg_one
					
					atime = arg[:attributes][:atime]
					mtime = arg[:attributes][:mtime]
					
					if atime[:seconds] == neg_one
						atime = attrs[:atime]
					end
					if mtime[:seconds] == neg_one
						mtime = attrs[:mtime]
					end
					
					attrs[:atime] = atime
					attrs[:mtime] = mtime
					
					atime = Time.at(atime[:seconds], atime[:useconds])
					mtime = Time.at(mtime[:seconds], mtime[:useconds])
					
					f.utime(atime, mtime)
				end
				
				{
					:_discriminant => :NFS_OK,
					:attributes => attrs
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
				f = @fh_table[arg[:dir][:data]].lookup(arg[:name])
				fh = add_filehandle(f)
				attrs = f.lstat

				result = {
					:_discriminant => :NFS_OK,
					:diropres => {
						:file => {
							:data => fh
						},
						:attributes => convert_attrs(attrs)
					}
				}

				puts result.inspect

				result
			end
		end
		
		@nfs_prog.on_call(@nfs_vers, :READLINK) do |arg, auth, verf|
			puts 'READLINK'
			puts arg.inspect
			
			handle_errors do
				result = @fh_table[arg[:data]].readlink

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
				f = @fh_table[arg[:file][:data]]
				attrs = f.lstat
				f.pos = arg[:offset]
				result = f.read(arg[:count])

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
			puts arg.inspect
			
			handle_errors do
				f = @fh_table[arg[:file][:data]]
				f.pos = arg[:offset]
				f.write(arg[:data])
				f.flush
				attrs = f.lstat
				
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
				dir = @fh_table[arg[:where][:dir][:data]]
				name = arg[:where][:name]
				
				f, attrs = dir.create(arg[:where][:name],
					arg[:attributes][:mode], arg[:attributes][:uid],
					arg[:attributes][:gid])
				fh = add_filehandle(f)
				
				{
					:_discriminant => :NFS_OK,
					:diropres => {
						:file => {
							:data => fh
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
				dir = @fh_table[arg[:dir][:data]]
				name = arg[:name]
				dir.unlink(name)
				
				{:_discriminant => :NFS_OK}
			end)[:_discriminant]
		end
		
		@nfs_prog.on_call(@nfs_vers, :RENAME) do |arg, auth, verf|
			puts 'RENAME'
			puts arg.inspect
			
			(handle_errors do
				from_dir = @fh_table[arg[:from][:dir][:data]]
				from_name = arg[:from][:name]
				to_dir = @fh_table[arg[:to][:dir][:data]]
				to_name = arg[:to][:name]

				from_dir.rename(from_name, to_dir, to_name)
				
				{:_discriminant => :NFS_OK}
			end)[:_discriminant]
		end
		
		@nfs_prog.on_call(@nfs_vers, :LINK) do |arg, auth, verf|
			puts 'LINK'
			puts arg.inspect
			
			(handle_errors do
				from = @fh_table[arg[:from][:data]]
				to_dir = @fh_table[arg[:to][:dir][:data]]
				to_name = arg[:to][:name]

				from.link(to_dir, to_name)
				
				{:_discriminant => :NFS_OK}
			end)[:_discriminant]
		end
		
		@nfs_prog.on_call(@nfs_vers, :SYMLINK) do |arg, auth, verf|
			puts 'SYMLINK'
			puts arg.inspect
			
			(handle_errors do
				dir = @fh_table[arg[:from][:dir][:data]]
				name = arg[:from][:name]
				to_name = arg[:to]
				attrs = arg[:attributes]

				dir.symlink(name, to_name)

				{:_discriminant => :NFS_OK}
			end)[:_discriminant]
		end
		
		@nfs_prog.on_call(@nfs_vers, :MKDIR) do |arg, auth, verf|
			puts 'MKDIR'
			puts arg.inspect
			
			handle_errors do
				dir = @fh_table[arg[:where][:dir][:data]]

				f, attrs = dir.mkdir(arg[:where][:name], arg[:attributes][:mode],
					arg[:attributes][:uid], arg[:attributes][:gid])
				fh = add_filehandle(f)

				{
					:_discriminant => :NFS_OK,
					:diropres => {
						:file => {
							:data => fh
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
				dir = @fh_table[arg[:dir][:data]]
				name = arg[:name]
				dir.rmdir(name)

				{:_discriminant => :NFS_OK}
			end)[:_discriminant]
		end
		
		@nfs_prog.on_call(@nfs_vers, :READDIR) do |arg, auth, verf|
			puts 'READDIR'
			puts arg.inspect
			
			handle_errors do
				dir = @fh_table[arg[:dir][:data]]

				cookie = arg[:cookie]
				count = arg[:count]

				need_bytes = 16 + 12

				entries = dir.entries

				result_entries = nil
				last_entry = nil

				while cookie < entries.size and need_bytes < count
					need_bytes += NFS::Filename.encode(entries[cookie]).size
					
					next_entry = {
						:fileid => 1,
						:name => entries[cookie],
						:cookie => cookie
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

				if not last_entry.nil?
					last_entry[:nextentry] = nil
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
