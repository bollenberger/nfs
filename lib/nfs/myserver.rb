require 'vcfs_server'

Thread.abort_on_exception = true

nfs = SUNRPC::UDPServer.new(
	VCFSServer.new('dbi:Pg:test').programs, 1234, '127.0.0.1')

nfs.join
