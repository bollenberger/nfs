require 'dbi'
require 'thread'

module VCFS

class File
end

class File::Stat
	include Comparable
	
	def initialize(ino, mode, uid, gid, size, nlink, atime, mtime, ctime)
		@ino, @mode, @uid, @gid, @size, @nlink, @atime, @mtime, @ctime =
			ino, mode, uid, gid, size, nlink, atime, mtime, ctime
	end
	
	def <=>(other)
		@mtime <=> other.mtime
	end
	
	attr_reader :atime, :ctime, :gid, :ino, :mtime, :nlink, :size, :uid
	
	def blksize
		1024
	end
	
	def blockdev?
		(@mode & 0170000) == 060000
	end
	
	def blocks
		size / blksize + 1
	end
	
	def chardev?
		(@mode & 0170000) == 020000
	end
	
	def dev
		0
	end
	
	def directory?
		(@mode & 0170000) == 040000
	end
	
	def file?
		(@mode & 0170000) == 0100000
	end
	
	def ftype
		if file?
			'file'
		elsif directory?
			'directory'
		elsif chardev?
			'characterSpecial'
		elsif block?
			'blockSpecial'
		elsif pipe?
			'fifo'
		elsif symlink?
			'link'
		elsif socket?
			'socket'
		end
	end
	
	def mode
		@mode
	end
	
	def pipe?
		(@mode & 0170000) == 010000
	end
	
	def rdev
		0
	end
	
	def setgid?
		(@mode & 02000) != 0
	end
	
	def setuid?
		(@mode & 04000) != 0
	end
	
	def size?
		if size == 0
			nil
		else
			size
		end
	end
	
	def socket?
		(@mode & 0170000) == 0140000
	end
	
	def sticky?
		(@mode & 01000) != 0
	end
	
	def symlink?
		(@mode & 0170000) == 0120000
	end
	
	def zero?
		size == 0
	end
end

class VCFS
	def initialize(*args)
		@mutex = Mutex.new
		@dbh = DBI.connect(*args)
		
		@mkbranch = @dbh.prepare('select mkbranch(?::integer, ?::timestamp,
			?::text)')
		@getattr = @dbh.prepare('select * from getattr(?::integer,
			?::integer)')
		@setattr = @dbh.prepare(
			'select * from setattr(?::integer, ?::integer, ?::integer,
			?::integer, ?::integer, ?::integer, ?::timestamp, ?::timestamp)')
		@mkfile = @dbh.prepare('select mkfile(?::integer, ?::integer,
			?::text, ?::integer, ?::integer, ?::integer)')
		@symlink = @dbh.prepare('select symlink(?::integer, ?::integer,
			?::text, ?::bytea, ?::integer, ?::integer, ?::integer)')
		@mkdir = @dbh.prepare('select mkdir(?::integer, ?::integer, ?::text,
			?::integer, ?::integer, ?::integer)')
		@rmdir = @dbh.prepare('select rmdir(?::integer, ?::integer, ?::text)')
		@lookup = @dbh.prepare('select lookup(?::integer, ?::integer,
			?::text)')
		@link = @dbh.prepare('select link(?::integer, ?::integer, ?::integer,
			?::text)')
		@unlink = @dbh.prepare('select unlink(?::integer, ?::integer,
			?::text)')
		@rename = @dbh.prepare('select rename(?::integer, ?::integer, ?::text,
			?::integer, ?::text)')
		@read = @dbh.prepare('select read(?::integer, ?::integer, ?::integer,
			?::integer)')
		@write = @dbh.prepare('select write(?::integer, ?::integer, ?::integer,
			?::bytea)')
		@readdir = @dbh.prepare('select * from readdir(?::integer, ?::integer)
			order by name')
		
		if block_given?
			begin
				yield(self)
			ensure
				disconnect
			end
		end
	end
	
	def disconnect
		@dbh.disconnect if @dbh
	end
	
	def handle_errors
		begin
			@mutex.synchronize do
				yield
			end
		rescue DBI::ProgrammingError => e
			message = e.errstr.split(':', 2)
			if message.size >= 2
				message = message[1].strip
			else
				message = ''
			end
			message = message.split("\n", 2)
			if message.size >= 1
				message = message[0].strip
			else
				message = ''
			end
			puts message.inspect
			if message == 'EPERM'
				raise Errno::EPERM
			elsif message == 'ENOENT'
				raise Errno::ENOENT
			elsif message == 'EIO'
				raise Errno::EIO
			elsif message == 'ENXIO'
				raise Errno::ENXIO
			elsif message == 'EACCES'
				raise Errno::EACCES
			elsif message == 'EEXIST'
				raise Errno::EEXIST
			elsif message == 'ENODEV'
				raise Errno::ENODEV
			elsif message == 'ENOTDIR'
				raise Errno::ENOTDIR
			elsif message == 'EISDIR'
				raise Errno::EISDIR
			elsif message == 'EINVAL'
				raise Errno::EINVAL
			elsif message == 'EFBIG'
				raise Errno::EFBIG
			elsif message == 'ENOSPC'
				raise Errno::ENOSPC
			elsif message == 'EROFS'
				raise Errno::EROFS
			elsif message == 'ENAMETOOLONG'
				raise Errno::ENAMETOOLONG
			elsif message == 'ENOTEMPTY'
				raise Errno::ENOTEMPTY
			elsif message == 'EDQUOT'
				raise Errno::EDQUOT
			elsif message == 'ESTALE'
				raise Errno::ESTALE
			else
				raise
			end
		end
	end
	
	def mkbranch(parent, name=nil, created='now')
		handle_errors do
			@mkbranch.execute(parent, created, name)
			result = @mkbranch.fetch[0]
			@mkbranch.cancel
			result
		end
	end
	
	def getattr(branch, file)
		handle_errors do
			@getattr.execute(branch, file)
			result = @getattr.fetch
			result = File::Stat.new(file, result['r_mode'], result['r_uid'],
				result['r_gid'], result['r_size'], result['r_nlink'],
				result['r_atime'].to_time, result['r_mtime'].to_time,
				result['r_ctime'].to_time)
			@getattr.cancel
			result
		end
	end
	
	def setattr(branch, file, mode=nil, uid=nil, gid=nil, size=nil,
		atime=nil, mtime=nil)
		
		handle_errors do
			@setattr.execute(branch, file, mode, uid, gid, size, atime, mtime)
			result = @setattr.fetch
			result = File::Stat.new(file, result['p_mode'], result['p_uid'],
				result['p_gid'], result['p_size'], result['r_nlink'],
				result['p_atime'].to_time, result['p_mtime'].to_time,
				result['r_ctime'].to_time)
			@setattr.cancel
			result
		end
	end
	
	def mkfile(branch, dir, name, mode, uid, gid)
		handle_errors do
			@mkfile.execute(branch, dir, name, mode, uid, gid)
			result = @mkfile.fetch[0]
			@mkfile.cancel
			result
		end
	end
	
	def symlink(branch, dir, name, to, mode, uid, gid)
		handle_errors do
			@symlink.execute(branch, dir, name, to, mode, uid, gid)
		end
	end
	
	def mkdir(branch, dir, name, mode, uid, gid)
		handle_errors do
			@mkdir.execute(branch, dir, name, mode, uid, gid)
			result = @mkdir.fetch[0]
			@mkdir.cancel
			result
		end
	end
	
	def rmdir(branch, dir, name)
		handle_errors do
			@rmdir.execute(branch, dir, name)
			@rmdir.cancel
			nil
		end
	end
	
	def lookup(branch, dir, name)
		handle_errors do
			@lookup.execute(branch, dir, name)
			result = @lookup.fetch[0]
			@lookup.cancel
			result
		end
	end
	
	def link(branch, file, dir, name)
		handle_errors do
			@link.execute(branch, file, dir, name)
			@link.cancel
			nil
		end
	end
	
	def unlink(branch, dir, name)
		handle_errors do
			@unlink.execute(branch, dir, name)
			@unlink.cancel
			nil
		end
	end
	
	# Warning. Make sure that you don't try to rename across branches
	def rename(branch, from_dir, from_name, to_dir, to_name)
		handle_errors do
			@rename.execute(branch, from_dir, from_name, to_dir, to_name)
			@rename.cancel
			nil
		end
	end
	
	def read(branch, file, start, length)
		handle_errors do
			@read.execute(branch, file, start, length)
			result = @read.fetch[0]
			@read.cancel
			result
		end
	end
	
	def write(branch, file, start, data)
		handle_errors do
			@write.execute(branch, file, start, escape_bytea(data))
			@write.cancel
			nil
		end
	end
	
	def readdir(branch, dir)
		result = []
		handle_errors do
			@readdir.execute(branch, dir)
			@readdir.fetch do |row|
				result << row[0]
			end
			@readdir.cancel
		end
		result
	end

private

	def escape_bytea(binstr)
		binstr.split(//).map do |char|
			case char[0]
				when (0..31),39,92,(127..255)
					"\\#{sprintf("%03o", char[0])}"
				else
					char
			end
  		end.join
	end
end

end
