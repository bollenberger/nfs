# Ruby XDR based implementation of SUNRPC. Based on RFC 1057.
#
# Author: Brian Ollenberger

require 'nfs/xdr'
require 'socket'
require 'thread'

module SUNRPC

include XDR

MAXAUTHLEN = 400
AUTH_UNIX_MAXMACHINENAMELEN = 255
AUTH_UNIX_MAXGIDS = 16

auth_flavor = Enumeration.new do
	name :AUTH_NULL, 0
	name :AUTH_UNIX, 1
	name :AUTH_SHORT, 2
	name :AUTH_DES, 3
	# and more to be defined?
end

Opaque_auth = Structure.new do
	component :flavor, auth_flavor
	component :body, Opaque.new(MAXAUTHLEN)
end

Auth_unix = Structure.new do
	component :stamp, UnsignedInteger.new
	component :machinename, String.new(AUTH_UNIX_MAXMACHINENAMELEN)
	component :uid, UnsignedInteger.new
	component :gid, UnsignedInteger.new
	component :gids, Array.new(UnsignedInteger.new,
		AUTH_UNIX_MAXGIDS)
end

authdes_namekind = Enumeration.new do
	name :ADN_FULLNAME, 0
	name :ADN_NICKNAME, 1
end

des_block = FixedOpaque.new(8)

MAXNETNAMELEN = 255

authdes_fullname = Structure.new do
	component :name, String.new(MAXNETNAMELEN) # name of client
	component :key, des_block                  # PK encrypted conversation key
	component :window, FixedOpaque.new(4)      # encrypted window
end

authdes_cred = Union.new(authdes_namekind) do
	arm :ADN_FULLNAME do
		component :adc_fullname, authdes_fullname
	end
	
	arm :ADN_NICKNAME do
		component :adc_nickname, Integer.new
	end
end

timestamp = Structure.new do
	component :seconds, UnsignedInteger.new  # seconds
	component :useconds, UnsignedInteger.new # microseconds
end

authdes_verf_clnt = Structure.new do
	component :adv_timestamp, des_block        # encrypted timestamp
	component :adv_winverf, FixedOpaque.new(4) # encrypted window verifier
end

authdes_verf_svr = Structure.new do
	component :adv_timeverf, des_block   # encrypted verifier
	component :adv_nickname, Integer.new # nickname for client (unencrypted)
end

msg_type = Enumeration.new do
	name :CALL, 0
	name :REPLY, 1
end

reply_stat = Enumeration.new do
	name :MSG_ACCEPTED, 0
	name :MSG_DENIED, 1
end

accept_stat = Enumeration.new do
	name :SUCCESS, 0       # RPC executed successfully
	name :PROG_UNAVAIL, 1  # remote hasn't exported program
	name :PROG_MISMATCH, 2 # remote can't support version number
	name :PROC_UNAVAIL, 3  # program can't support procedure
	name :GARBAGE_ARGS, 4  # procedure can't decode params
end

reject_stat = Enumeration.new do
	name :RPC_MISMATCH, 0 # RPC version number != 2
	name :AUTH_ERROR, 1   # remote can't authenticate caller
end

auth_stat = Enumeration.new do
	name :AUTH_BADCRED, 1      # bad credentials (seal broken)
	name :AUTH_REJECTEDCRED, 2 # client must begin new session
	name :AUTH_BADVERF, 3      # bad verifier (seal broken)
	name :AUTH_REJECTEDVERF, 4 # verifier expired or replayed
	name :AUTH_TOOWEAK, 5      # rejected for security reasons
end

call_body = Structure.new do
	component :rpcvers, UnsignedInteger.new # must be equal to two (2)
	component :prog, UnsignedInteger.new
	component :vers, UnsignedInteger.new
	component :proc, UnsignedInteger.new
	component :cred, Opaque_auth
	component :verf, Opaque_auth
	# procedure specific parameters start here
end

accepted_reply = Structure.new do
	component :verf, Opaque_auth
	component :reply_data, Union.new(accept_stat) do
		arm :SUCCESS do
			component :results, FixedOpaque.new(0)
			# Procedure specific results start here
		end
		
		arm :PROG_MISMATCH do
			component :mismatch_info, Structure.new do
				component :low, UnsignedInteger.new
				component :high, UnsignedInteger.new
			end
		end
		
		default do
			# Void. Cases include PROG_UNAVAIL, PROC_UNAVAIL, and GARBAGE_ARGS.
		end
	end
end

rejected_reply = Union.new(reject_stat) do
	arm :RPC_MISMATCH do
		component :mismatch_info, Structure.new do
			component :low, UnsignedInteger.new
			component :high, UnsignedInteger.new
		end
	end
	
	arm :AUTH_ERROR do
		component :stat, auth_stat
	end
end

reply_body = Union.new(reply_stat) do
	arm :MSG_ACCEPTED do
		component :areply, accepted_reply
	end
	
	arm :MSG_DENIED do
		component :rreply, rejected_reply
	end
end

Rpc_msg = Structure.new do
	component :xid, UnsignedInteger.new
	component :body, (Union.new(msg_type) do
		arm :CALL do
			component :cbody, call_body
		end
		
		arm :REPLY do
			component :rbody, reply_body
		end
	end)
end

# Define RPC language

class Program
	def initialize(number, &block)
		@number = number
		@versions = {}
		@low = @high = nil
		
		if block_given?
			instance_eval(&block)
		end
	end
	
	attr_reader :number, :low, :high
	
	def dup
		p = Program.new(@number)
		@versions.each_pair do |number, version|
			p.add_version(number, version.dup)
		end
		p
	end
	
	def add_version(number, ver)
		if @low.nil? or number < @low
			@low = number
		end
		if @high.nil? or number > @high
			@high = number
		end
		
		@versions[number] = ver
	end
	
	def version(ver, &block)
		add_version(ver, Version.new(ver, &block))
	end
	
	def get_version(ver)
		@versions[ver]
	end
	
	def each_version(&block)
		@versions.each_value(&block)
	end
	
	def on_call(ver, procedure_name, &block)
		@versions[ver].on_call(procedure_name, &block)
	end
	
	def call(ver, procedure, arg, cred, verf)
		if not @versions.has_key?(ver)
			raise ProgramMismatch
		end
		
		@versions[ver].call(procedure, arg, cred, verf)
	end
end

class Version
	def initialize(number, &block)
		@number = number
		@procedures = {}
		@procedure_names = {}
		
		# Add the customary null procedure by default.
		procedure SUNRPC::Void.new, :NULL, 0, SUNRPC::Void.new do
			# do nothing
		end
		
		if block_given?
			instance_eval(&block)
		end
	end
	
	attr_reader :number
	
	def dup
		v = Version.new(@number)
		@procedure_names.each_pair do |name, procedure|
			v.add_procedure(name, procedure.number, procedure.dup)
		end
		v
	end
	
	def add_procedure(name, number, newproc)
		@procedures[number] = newproc
		@procedure_names[name] = newproc
	end
	
	# The name is required, but just for documentation.
	def procedure(returntype, name, number, argtype, &block)
		newproc = Procedure.new(number, returntype, argtype, &block)
		add_procedure(name, number, newproc)
	end
	
	def get_procedure(procedure_name)
		@procedure_names[procedure_name]
	end
	
	def on_call(procedure_name, &block)
		@procedure_names[procedure_name].on_call(&block)
	end
	
	def call(p, arg, cred, verf)
		if not @procedures.has_key?(p)
			raise ProcedureUnavailable
		end
		
		@procedures[p].call(arg, cred, verf)
	end
end

class Procedure
	def initialize(number, returntype, argtype, &block)
		@number = number
		@returntype, @argtype = returntype, argtype
		@block = block
	end
	
	attr_reader :number
	
	def dup
		Procedure.new(@number, @returntype, @argtype, &@block)
	end
	
	def on_call(&block)
		@block = block
	end
	
	def encode(arg)
		@argtype.encode(arg)
	end
	
	def decode(value)
		@returntype.decode(value)
	end
	
	def call(arg, cred, verf)
		begin
			arg_object = @argtype.decode(arg)
		rescue
			raise GarbageArguments
		end
		
		# Undefined procedures are also unavailable, even if the XDR says it's
		# there. Define your procedures and this won't happen.
		if @block.nil?
			raise ProcedureUnavailable
		end
		
		result_object = @block.call(arg_object, cred, verf)
		
		result = nil
		begin
			result = @returntype.encode(result_object)
		rescue => e
			puts e
			print e.backtrace.join("\n")
			# TODO LOG
			raise IgnoreRequest
		end
		result
	end
end

# Transport layer

module Client
	@@xid = 0
	@@xid_mutex = Mutex.new
	
	def method_missing(name, *args)
		procedure = @version.get_procedure(name)
		if procedure.nil?
			raise 'NoMethodError: ' + name.to_s
		end
		
		if args.size == 0
			args = [nil]
		end
		
		if args.size != 1
			raise ArgumentError
		end
		
		xid = nil
		@@xid_mutex.synchronize do
			xid = @@xid
			@@xid += 1
		end
		
		message = Rpc_msg.encode({
			:xid => xid,
			:body => {
				:_discriminant => :CALL,
				:cbody => {
					:rpcvers => 2,
					:prog => @program.number,
					:vers => @version.number,
					:proc => procedure.number,
					:cred => {
						:flavor => :AUTH_NULL,
						:body => ''
					},
					:verf => {
						:flavor => :AUTH_NULL,
						:body => ''
					}
				}
			}
		}) + procedure.encode(args[0])
		
		# This will return the result object or raise an exception that
		# contains the cause of the error.
		sendrecv(message) do |result|
			envelope = Rpc_msg.decode(result)
			if envelope[:xid] == xid
				if envelope[:body][:_discriminant] != :REPLY
					raise envelope.inspect
				end
				
				if envelope[:body][:rbody][:_discriminant] != :MSG_ACCEPTED
					raise envelope[:body][:rbody].inspect
				end
				
				if envelope[:body][:rbody][:areply][:reply_data] \
					[:_discriminant] != :SUCCESS
					
					raise envelope[:body][:rbody][:areply][:reply_data].inspect
				end
				
				procedure.decode(result)
			else
				false # false means keep giving us received messages to inspect
			end
		end
	end
end

# Server Exceptions

class IgnoreRequest < Exception
	def encode(xid)
		nil
	end
end

# Abstract base of "rejected" errors
class RequestDenied < Exception
	def encode(xid)
		Rpc_msg.encode({
			:xid => xid,
			:body => {
				:_discriminant => :REPLY,
				:rbody => {
					:_discriminant => :MSG_DENIED,
					:rreply => rreply
				}
			}
		})
	end
end

class RpcMismatch < RequestDenied
	# RPC mismatch takes the xid since, it won't actually have one
	# passed to its encode method.
	def initialize(low, high, xid)
		@low, @high, @xid = low, high, xid
	end
	
	def encode(xid)
		Rpc_msg.encode({
			:xid => @xid,
			:body => {
				:_discriminant => :REPLY,
				:rbody => {
					:_discriminant => :MSG_DENIED,
					:rreply => rreply
				}
			}
		})
	end
	
private
	
	def rreply
		{
			:_discriminant => :RPC_MISMATCH,
			:mismatch_info => {
				:low => @low,
				:high => @high
			}
		}
	end
end

# Abstract base of authentication errors
class AuthenticationError < RequestDenied
private
	def rreply
		{
			:_discriminant => :AUTH_ERROR,
			:stat => auth_stat
		}
	end
end

class BadCredentials < AuthenticationError
private
	def auth_stat
		:AUTH_BADCRED
	end
end

class RejectedCredentials < AuthenticationError
private
	def auth_stat
		:AUTH_REJECTEDCRED
	end
end

class BadVerifier < AuthenticationError
private
	def auth_stat
		:AUTH_BADVERF
	end
end

class RejectedVerifier < AuthenticationError
private
	def auth_stat
		:AUTH_REJECTEDVERF
	end
end

class TooWeak < AuthenticationError
private
	def auth_stat
		:AUTH_TOOWEAK
	end
end

# Abstract base of errors where the message was "accepted"
class AcceptedError < Exception
	def encode(xid)
		Rpc_msg.encode({
			:xid => xid,
			:body => {
				:_discriminant => :REPLY,
				:rbody => {
					:_discriminant => :MSG_ACCEPTED,
					:areply => areply
				}
			}
		})
	end
end

# Program not supported
class ProgramUnavailable < AcceptedError
private
	def areply
		{
			:verf => {:flavor => :AUTH_NULL, :body => ''},
			:reply_data => {
				:_discriminant => :PROG_UNAVAIL
			}
		}
	end
end

# Version not supported
class ProgramMismatch < AcceptedError
	def initialize(low, high)
		@low, @high = low, high
	end
	
private
	
	def areply
		{
			:verf => {:flavor => :AUTH_NULL, :body => ''},
			:reply_data => {
				:_discriminant => :PROG_MISMATCH,
				:low => @low,
				:high => @high
			}
		}
	end
end

# Procedure not supported
class ProcedureUnavailable < AcceptedError
private
	def areply
		{
			:verf => {:flavor => :AUTH_NULL, :body => ''},
			:reply_data => {
				:_discriminant => :PROC_UNAVAIL
			}
		}
	end
end

class GarbageArguments < AcceptedError
private
	def areply
		{
			:verf => {:flavor => :AUTH_NULL, :body => ''},
			:reply_data => {
				:_discriminant => :GARBAGE_ARGS
			}
		}
	end
end

module Server
	def decode_envelope(data)
		envelope = nil
		begin
			envelope = Rpc_msg.decode(data)
		rescue
			raise IgnoreRequest
		end
		
		if envelope[:body][:_discriminant] != :CALL
			raise IgnoreRequest
		end
		
		if envelope[:body][:cbody][:rpcvers] != 2
			raise RpcMismatch.new(2, 2, envelope[:xid])
		end
		
		return envelope[:xid], envelope[:body][:cbody][:prog],
			envelope[:body][:cbody][:vers], envelope[:body][:cbody][:proc],
			envelope[:body][:cbody][:cred], envelope[:body][:cbody][:verf]
	end
	
	def create_success_envelope(xid, result)
		Rpc_msg.encode({
			:xid => xid,
			:body => {
				:_discriminant => :REPLY,
				:rbody => {
					:_discriminant => :MSG_ACCEPTED,
					:areply => {
						:verf => {
							:flavor => :AUTH_NULL,
							:body => ''
						},
						:reply_data => {
							:_discriminant => :SUCCESS,
							:results => ''
						}
					}
				}
			}
		}) + result
	end

private

	def hash_programs(programs)
		if programs.kind_of?(Hash)
			programs
		elsif programs.kind_of?(Array)
			result = {}
			programs.each do |program|
				result[program.number] = program
			end
			result
		else
			{programs.number => programs}
		end
	end
end

# UDP transport

UDPRecvMTU = 10000
DefaultPort = 12345

class UDPClient
	include Client
	
	def initialize(program, version, port, address = '127.0.0.1')
		if address.kind_of?(UDPSocket)
			@socket = address
		else
			@socket = UDPSocket.open
			@socket.connect(address, port)
		end
		
		@program = program
		@version = program.get_version(version)
		
		@socket_mutex = Mutex.new
		
		if block_given?
			begin
				yield(self)
			ensure
				shutdown
			end
		end
	end
	
	def sendrecv(send)
		result = false
		@socket_mutex.synchronize do
			@socket.send(send, 0)
		
			while result == false
				buffer = @socket.recv(UDPRecvMTU)
				result = yield(buffer)
			end
		end
		result
	end
	
	def shutdown
		@socket.close
	end
end

class UDPServer
	include Server
	
	def initialize(programs, port = nil, address = '0.0.0.0')
		if address.kind_of?(UDPSocket)
			@socket = address
		else
			@socket = UDPSocket.open
			@socket.bind(address, port)
		end
		
		socketmutex = Mutex.new
		
		@programs = hash_programs(programs)
		
		@thread = Thread.new do
			request = nil
			while true
				request = @socket.recvfrom(UDPRecvMTU)
				data = request[0]
				port = request[1][1]
				address = request[1][3]
				
				Thread.new do
					result = nil
					xid = nil
					begin
						xid, program_num, version_num, procedure_num, cred,
							verf = decode_envelope(data)
						
						program = @programs[program_num]
						if program.nil?
							raise ProgramUnavailable
						else
							result = program.call(
								version_num, procedure_num, data, cred, verf)
							result = create_success_envelope(xid, result)
						end
					rescue IgnoreRequest, RequestDenied, AcceptedError => e
						result = e.encode(xid)
					end
					
					if not result.nil?
						socketmutex.synchronize do
							@socket.send(result, 0, address, port)
						end
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
	
	def port
		@socket.addr[1]
	end
	
	def join
		if not @thread.nil?
			@thread.join
		end
	end
	
	def shutdown
		Thread.kill(@thread)
		@thread = nil
		@socket.close
	end
end

class TCPServer
	include Server
	
	def defragment_data(client)
		eom_found = false
		data = ""

		#puts "beginning"

		begin
			header = client.recv(4)
			#puts "[defragment_data] header = #{header.inspect}"

			return nil if header.size != 4
			header = header.unpack('N')[0]
			eom_found = ((header & 0x80000000) == 0x80000000)
			expected = header & 0x7fffffff

			partial = client.recv(expected)
			#puts "[defragment_data] eom_found = #{eom_found}, expected = #{expected}, partial = #{partial.inspect}"

			return nil if partial.size != expected

			data += partial
		end while not eom_found 

		#puts "[defragment_data] returning"

		data

	end

	def initialize(programs, port = nil, address = '0.0.0.0')
		if address.kind_of?(TCPServer)
			@socket = address
		else
			@socket = ::TCPServer.open(address, port)
		end
		
		@programs = hash_programs(programs)


		
		@thread = Thread.new do
			while true
				client = @socket.accept

				Thread.new do
					begin
						begin
							result = nil
							data = defragment_data(client)

							break if data.nil?

							xid = nil
							begin
								xid, program_num, version_num, procedure_num, cred,
								verf = decode_envelope(data)
						
								program = @programs[program_num]
								if program.nil?
									raise ProgramUnavailable
								else
									result = program.call(
									version_num, procedure_num, data, cred, verf)
									result = create_success_envelope(xid, result)
								end
							rescue IgnoreRequest, RequestDenied, AcceptedError => e
								result = e.encode(xid)
							end

							if not result.nil?
								client.send([ 0x80000000 | result.length].pack('N') + result, 0)
							end
						end while true
					ensure
						client.close
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
	
	def port
		@socket.addr[1]
	end
	
	def join
		if not @thread.nil?
			@thread.join
		end
	end
	
	def shutdown
		Thread.kill(@thread)
		@thread = nil
		@socket.shutdown
	end
end
end
