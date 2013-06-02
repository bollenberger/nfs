# Helpers to simplify the creation of SUNRPC clients and servers that
# are clients of portmap.
#
# Author: Brian Ollenberger

require 'nfs/portmap'
require 'nfs/sunrpc'

module PortmapTransport

class UDPClient < SUNRPC::UDPClient
	def initialize(program, version, address = '127.0.0.1',
		portmap_port = Portmap::PMAP_PORT)
		
		# Ask portmap for the port.
		client = SUNRPC::UDPClient.new(Portmap::PMAP_PROG, Portmap::PMAP_VERS,
			portmap_port, address)
		port = client.GETPORT({
			:prog => program.number,
			:vers => version,
			:prot => :UDP_IP,
			:port => 0
		})
		client.shutdown
		
		super(program, version, port, address)
	end
end

def PortmapTransport::make_server_shutdown_finalizer(
	programs, port, portmap_port)
	
	proc do |id|
		# Unregister the programs from the portmap server
		SUNRPC::UDPClient.new(Portmap::PMAP_PROG,
			Portmap::PMAP_VERS, portmap_port, '127.0.0.1') do |portmap_client|
			
			programs.each_value do |program|
				program.each_version do |version|
					portmap_client.UNSET({
						:prog => program.number,
						:vers => version.number,
						:prot => :UDP_IP,
						:port => port
					})
					portmap_client.UNSET({
						:prog => program.number,
						:vers => version.number,
						:prot => :TCP_IP,
						:port => port
					})
				end
			end
		end
	end
end

class UDPServer < SUNRPC::UDPServer
	def initialize(programs, listen_port = nil, address = '0.0.0.0',
		portmap_port = Portmap::PMAP_PORT)
		
		super(programs, listen_port, address, &nil)
		
		# Tell portmap about each version of each program that we have.
				
		ObjectSpace.define_finalizer(self,
			PortmapTransport::make_server_shutdown_finalizer(
				@programs, self.port, portmap_port))
		
		SUNRPC::UDPClient.new(Portmap::PMAP_PROG,
			Portmap::PMAP_VERS, portmap_port, '127.0.0.1') do |portmap_client|
			
			@programs.each_value do |program|
				program.each_version do |version|
					if :TRUE != portmap_client.SET({
						:prog => program.number,
						:vers => version.number,
						:prot => :UDP_IP,
						:port => self.port})
						
						raise 'unable to add port mapping for program ' +
							program.number.to_s + ' version ' +
							version.number.to_s
					end
				end
			end
		end
		
		if block_given?
			begin
				yield(self)
			ensure
				shutdown
			end
		end
	end
	
	def shutdown
		# @portmap_shutdown.shutdown
		super
	end
end

class TCPServer < SUNRPC::TCPServer
	def initialize(programs, listen_port = nil, address = '0.0.0.0',
		portmap_port = Portmap::PMAP_PORT)
		
		super(programs, listen_port, address, &nil)
		
		# Tell portmap about each version of each program that we have.
				
		ObjectSpace.define_finalizer(self,
			PortmapTransport::make_server_shutdown_finalizer(
				@programs, self.port, portmap_port))
		
		SUNRPC::UDPClient.new(Portmap::PMAP_PROG,
			Portmap::PMAP_VERS, portmap_port, '127.0.0.1') do |portmap_client|
			
			@programs.each_value do |program|
				program.each_version do |version|
					if :TRUE != portmap_client.SET({
						:prog => program.number,
						:vers => version.number,
						:prot => :TCP_IP,
						:port => self.port})
						
						raise 'unable to add port mapping for program ' +
							program.number.to_s + ' version ' +
							version.number.to_s
					end
				end
			end
		end
		
		if block_given?
			begin
				yield(self)
			ensure
				shutdown
			end
		end
	end
	
	def shutdown
		# @portmap_shutdown.shutdown
		super
	end
end

end
