# Ruby XDR. XDR codec for Ruby. Based on RFC 4506.
#
# Author: Brian Ollenberger

module XDR

class Void
	def encode(value)
		''
	end
	
	def decode(string)
		nil
	end
end

class Integer
	def encode(value)
		[value].pack('N')
	end
	
	def decode(string)
		string.slice!(0..3).unpack('N').pack('I').unpack('i')[0]
	end
end

class UnsignedInteger
	def encode(value)
		[value].pack('N')
	end
	
	def decode(string)
		string.slice!(0..3).unpack('N')[0]
	end
end

class Enumeration < Integer
	def initialize(&block)
		@values = {}
		@names = {}
		
		if block_given?
			instance_eval(&block)
		end
	end
	
	def name(v_name, value)
		@values[v_name] = value
		@names[value] = v_name
	end
	
	def encode(name)
		super(@values[name])
	end
	
	def decode(string)
		@names[super(string)]
	end
end

class Boolean < Enumeration
	def initialize
		super
		name :TRUE, 1
		name :FALSE, 0
	end
end

class Float
	def encode(value)
		[value].pack('g')
	end
	
	def decode(string)
		string.slice!(0..3).unpack('g')[0]
	end
end

class Double
	def encode(value)
		[value].pack('G')
	end
	
	def decode(string)
		string.slice!(0..7).unpack('G')[0]
	end
end

def XDR::pad(n, align)
	r = n % align
	if r == 0
		r = align
	end
	n + align - r
end

class FixedString
	def initialize(n)
		@n = n
	end
	
	def encode(value)
		[value.to_s].pack('a' + XDR::pad(@n, 4).to_s)
	end
	
	def decode(string)
		superstring = string.slice!(0, XDR::pad(@n, 4))
		if superstring.nil?
			''
		else
			superstring[0, @n]
		end
	end
end

class String
	def initialize(n = nil)
		@n = n
		@length = UnsignedInteger.new
	end
	
	def encode(value)
		value = value.to_s
		n = value.size
		if not @n.nil? and @n < n
			n = @n
		end
		@length.encode(n) + [value].pack('a' + XDR::pad(n, 4).to_s)
	end
	
	def decode(string)
		length = @length.decode(string)
		superstring = string.slice!(0, XDR::pad(length, 4))
		if superstring.nil?
			''
		else
			superstring[0, length]
		end
	end
end

class FixedOpaque < FixedString
end

class Opaque < String
end

class FixedArray
	def initialize(type, n)
		@type, @n = type, n
	end
	
	def encode(value)
		n.times do |i|
			@type.encode(value[i])
		end
	end
	
	def decode(string)
		result = []
		n.times do
			result << @type.decode(string)
		end
		result
	end
end

class Array
	def initialize(type, n)
		@type, @n = type, n
		@length = UnsignedInteger.new
	end
	
	def encode(value)
		n = value.size
		if not @n.nil? and @n < n
			n = @n
		end
		result = @length.encode(n)
		n.times do |i|
			result << @type.encode(value[i])
		end
		result
	end
	
	def decode(string)
		length = @length.decode(string)
		result = []
		length.times do
			result << @type.decode(string)
		end
		result
	end
end

class Optional < Array
	def initialize(type)
		super(type, 1)
	end
	
	def encode(value)
		if value.nil?
			super([])
		else
			super([value])
		end
	end
	
	def decode(string)
		result = super(string)
		if result.empty?
			nil
		else
			result[0]
		end
	end
end

class Structure
	def initialize(&block)
		@components = []
		@names = []
		
		if block_given?
			instance_eval(&block)
		end
	end
	
	def component(name, type)
		@components << [name, type]
		@names << name
	end
	
	def encode(value)
		result = ''
		@components.each do |component|
			if not value.has_key?(component[0])
				raise 'missing structure component ' + component[0].to_s
			end
			
			result << component[1].encode(value[component[0]])
		end
		result
	end
	
	def decode(string)
		result = {}
		@components.each do |component|
			result[component[0]] = component[1].decode(string)
		end
		result
	end
end

# Each arm of the union is represented as a struct
class Union
	def initialize(discType, &block)
		@discType = discType
		@arms = {}
		@defaultArm = nil
		
		if block_given?
			instance_eval(&block)
		end
	end
	
	# Add an arm
	def arm(discValue, struct = nil, &block)
		if block_given?
			struct = Structure.new(&block)
		end
		
		@arms[discValue] = struct
	end
	
	# Set the default arm
	def default(struct = nil, &block)
		if block_given?
			struct = Structure.new(&block)
		end
		
		@defaultArm = struct
	end
	
	def encode(struct)
		disc = struct[:_discriminant]
		
		arm = @defaultArm
		if @arms.has_key?(disc)
			arm = @arms[disc]
		end
		
		result = @discType.encode(disc)
		if not arm.nil?
			result << arm.encode(struct)
		end
		result
	end
	
	def decode(string)
		disc = @discType.decode(string)
		
		arm = @defaultArm
		if @arms.has_key?(disc)
			arm = @arms[disc]
		end
		
		result = nil
		if not arm.nil?
			result = arm.decode(string)
		else
			result = {}
		end
		result[:_discriminant] = disc
		result
	end
end

end
