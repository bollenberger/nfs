require 'portmap'

class PortmapServer
	def initialize
		@portmap_prog = Portmap::PMAP_PROG.dup
		@portmap_vers = Portmap::PMAP_VERS
		
		@portmap = {}
		
		define_procedures
	end
	
	def programs
		[@portmap_prog]
	end
	
	def define_procedures
		@portmap_prog.on_call(@portmap_vers, :SET) do |arg, cred, verf|
			key = [arg[:prog], arg[:vers]]
			if @portmap.has_key?(key) and @portmap[key].has_key?(arg[:prot])
				:FALSE
			else
				@portmap[key] = {}
				@portmap[key][arg[:prot]] = arg[:port]
				:TRUE
			end
		end
		
		@portmap_prog.on_call(@portmap_vers, :UNSET) do |arg, cred, verf|
			key = [arg[:prog], arg[:vers]]
			if @portmap.has_key?(key)
				@portmap.delete(key)
				:TRUE
			else
				:FALSE
			end
		end
		
		@portmap_prog.on_call(@portmap_vers, :GETPORT) do |arg, cred, verf|
			key = [arg[:prog], arg[:vers]]
			if @portmap.has_key?(key) and @portmap[key].has_key?(arg[:prot])
				@portmap[key][arg[:prot]]
			else
				0
			end
		end

		@portmap_prog.on_call(@portmap_vers, :DUMP) do |arg, cred, verf|
			list = nil
			@portmap.each_pair do |key, prots|
				prog = key[0]
				vers = key[1]
				prots.each_pair do |prot, port|
					list = {
						:map => {
							:prog => prog,
							:vers => vers,
							:prot => prot,
							:port => port
						},
						:next => list
					}
				end
			end
			list
		end
	end
end
