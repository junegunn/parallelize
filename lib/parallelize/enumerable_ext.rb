module Enumerable
	# Divides the Enumerable objects into pieces and execute with multiple threads
	# @return [Array] Threads.
	# @param [Fixnum] num_threads Number of concurrent threads
	# @param [Boolean] collect_exceptions If true, waits for all threads to complete even in case of exception, and throws ParallelException at the end. If false exception is immediately thrown.
	def peach num_threads, collect_exceptions = false, &block
		raise ArgumentError.new("Block not given") unless block_given?
		raise ArgumentError.new("Invalid number of threads") if num_threads < 1

		threads = []
		self.each_slice((self.count{true} / num_threads.to_f).ceil) do |slice|
			threads << 
				case block.arity
				when 2
					Thread.new(slice, threads.length) { |my_slice, thread_idx|
						my_slice.each { |e| yield e, thread_idx }
					}
				when 1
					Thread.new(slice) { |my_slice|
						my_slice.each { |e| yield e }
					}
				when 0, -1
					raise ArgumentError.new("Invalid arity: #{block.arity}") if
						RUBY_VERSION !~ /^1.8\./ && block.arity == -1
					Thread.new(slice) { |my_slice|
						my_slice.each { yield }
					}
				else
					raise ArgumentError.new("Invalid arity: #{block.arity}")
				end
		end

		exceptions = {}
		threads.each_with_index do |thr, idx|
			begin
				thr.join
			rescue Exception => e
				if collect_exceptions
					exceptions[idx] = e
				else
					raise e
				end
			end
		end

		if exceptions.empty?
			threads
		else
			raise ParallelException.new(exceptions)
		end
	end
end

